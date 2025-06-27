use crate::yellowstone::{YellowstoneProfile, YellowstoneConnector, YellowstoneSubscribeConfig, YellowstoneTableConfig};
use anyhow::anyhow;
use arroyo_operator::context::{SourceCollector, SourceContext};
use arroyo_operator::operator::SourceOperator;
use arroyo_operator::SourceFinishType;
use arroyo_rpc::formats::{BadData, Format, Framing};
use arroyo_rpc::grpc::rpc::{StopMode, TableConfig};
use arroyo_rpc::{ControlMessage, OperatorConfig};
use arroyo_state::global_table_config;
use arroyo_state::tables::global_keyed_map::GlobalKeyedView;
use arroyo_types::{SignalMessage, UserError, Watermark};
use async_trait::async_trait;
use bincode::{Decode, Encode};
use futures::StreamExt;
use serde_json::json;
use std::collections::HashMap;
use std::time::SystemTime;
use tokio::select;
use tracing::{debug, error, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::*;

#[derive(Clone, Debug, Encode, Decode, PartialEq, PartialOrd, Default)]
pub struct YellowstoneSourceState {
    pub last_slot: Option<u64>,
    pub subscription_id: Option<u64>,
}

pub struct YellowstoneSourceFunc {
    pub endpoint: String,
    pub x_token: Option<String>,
    pub subscribe_config: YellowstoneSubscribeConfig,
    pub format: Format,
    pub framing: Option<Framing>,
    pub bad_data: Option<BadData>,
    pub state: YellowstoneSourceState,
}

impl YellowstoneSourceFunc {
    pub fn from_config(
        profile: YellowstoneProfile,
        table: YellowstoneTableConfig,
        config: OperatorConfig,
    ) -> anyhow::Result<Self> {
        let endpoint = profile
            .endpoint
            .sub_env_vars()
            .map_err(|e| anyhow!("Failed to resolve endpoint: {}", e))?
            .to_string();

        let x_token = if let Some(token) = profile.x_token {
            Some(
                token
                    .sub_env_vars()
                    .map_err(|e| anyhow!("Failed to resolve x_token: {}", e))?
                    .to_string(),
            )
        } else {
            None
        };

        let subscribe_config = YellowstoneConnector::new().parse_table_config(table)?;

        let format = config
            .format
            .ok_or_else(|| anyhow!("Format must be configured for Yellowstone source"))?;

        Ok(Self {
            endpoint,
            x_token,
            subscribe_config,
            format,
            framing: config.framing,
            bad_data: config.bad_data,
            state: YellowstoneSourceState::default(),
        })
    }

    async fn create_client(&self) -> Result<GeyserGrpcClient<impl yellowstone_grpc_client::Interceptor>, UserError> {
        let mut client = GeyserGrpcClient::build_from_shared(self.endpoint.clone())
            .map_err(|e| UserError::new("Yellowstone connection error", &format!("Failed to create gRPC client: {}", e)))?;

        if let Some(ref token) = self.x_token {
            client = client.x_token(Some(token))?;
        }

        Ok(client)
    }

    async fn handle_control_message(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
        msg: Option<ControlMessage>,
    ) -> Option<SourceFinishType> {
        match msg? {
            ControlMessage::Checkpoint(c) => {
                debug!("Starting checkpointing {}", ctx.task_info.task_index);
                let s: &mut GlobalKeyedView<(), YellowstoneSourceState> = ctx
                    .table_manager
                    .get_global_keyed_state("s")
                    .await
                    .expect("couldn't get state for yellowstone");
                s.insert((), self.state.clone()).await;

                if self.start_checkpoint(c, ctx, collector).await {
                    return Some(SourceFinishType::Immediate);
                }
            }
            ControlMessage::Stop { mode } => {
                info!("Stopping Yellowstone source: {:?}", mode);
                match mode {
                    StopMode::Graceful => return Some(SourceFinishType::Graceful),
                    StopMode::Immediate => return Some(SourceFinishType::Immediate),
                }
            }
            ControlMessage::Commit { .. } => {
                unreachable!("sources shouldn't receive commit messages");
            }
            ControlMessage::LoadCompacted { compacted } => {
                ctx.load_compacted(compacted).await;
            }
            ControlMessage::NoOp => {}
        }
        None
    }

    async fn handle_update(
        &mut self,
        update: SubscribeUpdate,
        collector: &mut SourceCollector,
    ) -> Result<(), UserError> {
        // Update state based on the message
        if let Some(ref message) = update.message {
            match message {
                subscribe_update::Message::Slot(slot_update) => {
                    self.state.last_slot = Some(slot_update.slot);
                }
                subscribe_update::Message::Block(block_update) => {
                    if let Some(block) = &block_update.block {
                        self.state.last_slot = Some(block.slot);
                    }
                }
                subscribe_update::Message::BlockMeta(block_meta) => {
                    self.state.last_slot = Some(block_meta.slot);
                }
                _ => {} // Account and transaction updates don't have slot info directly
            }
        }

        // Convert update to JSON for serialization
        let json_data = match serde_json::to_vec(&update) {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to serialize Yellowstone update: {}", e);
                return Err(UserError::new(
                    "Serialization error",
                    &format!("Failed to serialize update: {}", e),
                ));
            }
        };

        // Deserialize and collect the data
        collector
            .deserialize_slice(&json_data, SystemTime::now(), None)
            .await?;

        if collector.should_flush() {
            collector.flush_buffer().await?;
        }

        Ok(())
    }

    async fn run_subscription(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> Result<SourceFinishType, UserError> {
        let mut client = self.create_client().await?;

        let request = SubscribeRequest {
            accounts: self.subscribe_config.accounts.clone().unwrap_or_default(),
            slots: self.subscribe_config.slots.clone().unwrap_or_default(),
            transactions: self.subscribe_config.transactions.clone().unwrap_or_default(),
            transactions_status: HashMap::default(),
            blocks: self.subscribe_config.blocks.clone().unwrap_or_default(),
            blocks_meta: self.subscribe_config.blocks_meta.clone().unwrap_or_default(),
            entry: self.subscribe_config.entry.clone().unwrap_or_default(),
            commitment: Some(CommitmentLevel::Confirmed as i32),
            accounts_data_slice: vec![],
            ping: None,
            from_slot: None,
        };

        info!("Starting Yellowstone gRPC subscription");
        
        let mut stream = client
            .subscribe_once(request)
            .await
            .map_err(|e| UserError::new("Subscription error", &format!("Failed to subscribe: {}", e)))?;

        info!("Yellowstone gRPC subscription established");

        loop {
            select! {
                update = stream.next() => {
                    match update {
                        Some(Ok(update)) => {
                            if let Err(e) = self.handle_update(update, collector).await {
                                error!("Error handling update: {:?}", e);
                                ctx.report_user_error(e).await;
                            }
                        }
                        Some(Err(e)) => {
                            error!("gRPC stream error: {}", e);
                            return Err(UserError::new(
                                "Stream error",
                                &format!("gRPC stream error: {}", e),
                            ));
                        }
                        None => {
                            warn!("gRPC stream ended");
                            return Ok(SourceFinishType::Graceful);
                        }
                    }
                }
                msg = ctx.control_rx.recv() => {
                    if let Some(finish_type) = self.handle_control_message(ctx, collector, msg).await {
                        return Ok(finish_type);
                    }
                }
            }

            // Yield control periodically
            tokio::task::yield_now().await;
        }
    }
}

#[async_trait]
impl SourceOperator for YellowstoneSourceFunc {
    fn name(&self) -> String {
        "YellowstoneSource".to_string()
    }

    fn tables(&self) -> HashMap<String, TableConfig> {
        global_table_config("s", "Yellowstone source state")
    }

    async fn on_start(&mut self, ctx: &mut SourceContext) {
        let s: &mut GlobalKeyedView<(), YellowstoneSourceState> = ctx
            .table_manager
            .get_global_keyed_state("s")
            .await
            .expect("couldn't get state for yellowstone");

        if let Some(state) = s.get(&()) {
            self.state = state.clone();
            info!("Restored Yellowstone state: {:?}", self.state);
        } else {
            info!("Starting Yellowstone source with fresh state");
        }
    }

    async fn run(
        &mut self,
        ctx: &mut SourceContext,
        collector: &mut SourceCollector,
    ) -> SourceFinishType {
        // Initialize deserializer for the configured format
        collector.initialize_deserializer(
            self.format.clone(),
            self.framing.clone(),
            self.bad_data.clone(),
            &[],
        );

        match self.run_subscription(ctx, collector).await {
            Ok(finish_type) => finish_type,
            Err(e) => {
                error!("Yellowstone source error: {:?}", e);
                ctx.report_user_error(e.clone()).await;
                panic!("{}: {}", e.name, e.details);
            }
        }
    }
} 