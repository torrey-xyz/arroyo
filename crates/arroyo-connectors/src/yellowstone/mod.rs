use crate::yellowstone::source::YellowstoneSourceFunc;
use anyhow::anyhow;
use arroyo_operator::connector::{Connection, Connector};
use arroyo_operator::operator::ConstructedOperator;
use arroyo_rpc::api_types::connections::{
    ConnectionProfile, ConnectionSchema, ConnectionType, TestSourceMessage, SchemaDefinition,
};
use arroyo_rpc::var_str::VarStr;
use arroyo_rpc::{ConnectorOptions, OperatorConfig};
use arroyo_types::UserError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use yellowstone_grpc_proto::prelude::*;

pub mod source;

const CONFIG_SCHEMA: &str = include_str!("./profile.json");
const TABLE_SCHEMA: &str = include_str!("./table.json");
const ICON: &str = include_str!("./yellowstone.svg");

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct YellowstoneProfile {
    pub endpoint: VarStr,
    pub x_token: Option<VarStr>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct YellowstoneTableConfig {
    pub endpoint: String,
    pub accounts: Option<Vec<String>>,
    pub programs: Option<Vec<String>>,
}

// Remove Serialize/Deserialize from this struct since it contains protobuf types
#[derive(Clone, Debug)]
pub struct YellowstoneSubscribeConfig {
    pub accounts: Option<HashMap<String, SubscribeRequestFilterAccounts>>,
    pub slots: Option<HashMap<String, SubscribeRequestFilterSlots>>,
    pub transactions: Option<HashMap<String, SubscribeRequestFilterTransactions>>,
    pub blocks: Option<HashMap<String, SubscribeRequestFilterBlocks>>,
    pub blocks_meta: Option<HashMap<String, SubscribeRequestFilterBlocksMeta>>,
    pub entry: Option<HashMap<String, SubscribeRequestFilterEntry>>,
}

pub struct YellowstoneConnector {}

impl YellowstoneConnector {
    pub fn new() -> Self {
        Self {}
    }

    fn parse_table_config(&self, table_config: YellowstoneTableConfig) -> anyhow::Result<YellowstoneSubscribeConfig> {
        let mut accounts_map = HashMap::new();
        let mut slots_map = HashMap::new();
        let mut transactions_map = HashMap::new();
        let mut blocks_map = HashMap::new();
        let mut blocks_meta_map = HashMap::new();
        let mut entry_map = HashMap::new();

        // Handle accounts configuration
        if let Some(ref accounts) = table_config.accounts {
            for (i, account) in accounts.iter().enumerate() {
                let filter = SubscribeRequestFilterAccounts {
                    account: vec![account.clone()],
                    owner: vec![],
                    filters: vec![],
                    nonempty_txn_signature: None,
                };
                accounts_map.insert(format!("account_{}", i), filter);
            }
        }

        // Handle programs configuration (as accounts with owner filter)
        if let Some(ref programs) = table_config.programs {
            for (i, program) in programs.iter().enumerate() {
                let filter = SubscribeRequestFilterAccounts {
                    account: vec![],
                    owner: vec![program.clone()],
                    filters: vec![],
                    nonempty_txn_signature: None,
                };
                accounts_map.insert(format!("program_{}", i), filter);
            }
        }

        Ok(YellowstoneSubscribeConfig {
            accounts: if accounts_map.is_empty() { None } else { Some(accounts_map) },
            slots: if slots_map.is_empty() { None } else { Some(slots_map) },
            transactions: if transactions_map.is_empty() { None } else { Some(transactions_map) },
            blocks: if blocks_map.is_empty() { None } else { Some(blocks_map) },
            blocks_meta: if blocks_meta_map.is_empty() { None } else { Some(blocks_meta_map) },
            entry: if entry_map.is_empty() { None } else { Some(entry_map) },
        })
    }

    async fn test_connection(&self, endpoint: String, x_token: Option<String>) -> Result<(), UserError> {
        use yellowstone_grpc_client::GeyserGrpcClient;
        use arroyo_types::UserError;

        let mut client = GeyserGrpcClient::build_from_shared(endpoint)
            .map_err(|e| UserError::new("Yellowstone connection error", &format!("Failed to create gRPC client: {:?}", e)))?;

        if let Some(token) = x_token {
            client = client.x_token(Some(&token))
                .map_err(|e| UserError::new("Yellowstone connection error", &format!("Failed to set x-token: {:?}", e)))?;
        }

        // Just test if we can build the client - actual connection will be tested when subscription starts
        Ok(())
    }
}

impl Connector for YellowstoneConnector {
    type ProfileT = YellowstoneProfile;
    type TableT = YellowstoneTableConfig;

    fn name(&self) -> &'static str {
        "yellowstone"
    }

    fn metadata(&self) -> arroyo_rpc::api_types::connections::Connector {
        arroyo_rpc::api_types::connections::Connector {
            id: "yellowstone".to_string(),
            name: "Yellowstone".to_string(),
            icon: ICON.to_string(),
            description: "Read from Solana via Yellowstone gRPC".to_string(),
            enabled: true,
            source: true,
            sink: false,
            testing: false,
            hidden: false,
            custom_schemas: true,
            connection_config: Some(CONFIG_SCHEMA.to_string()),
            table_config: TABLE_SCHEMA.to_string(),
        }
    }

    fn table_type(&self, _: Self::ProfileT, _: Self::TableT) -> ConnectionType {
        ConnectionType::Source
    }

    fn get_schema(
        &self,
        _profile: Self::ProfileT,
        _table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> Option<ConnectionSchema> {
        // If schema is provided, use it, otherwise create a default one
        if let Some(schema) = schema {
            return Some(schema.clone());
        }

        // Create a basic schema for Yellowstone data
        let json_schema = r#"
        {
            "type": "object",
            "properties": {
                "slot": {"type": "number"},
                "account": {"type": "string"},
                "data": {"type": "string"},
                "owner": {"type": "string"},
                "executable": {"type": "boolean"},
                "rent_epoch": {"type": "number"},
                "lamports": {"type": "number"}
            }
        }
        "#;

        Some(ConnectionSchema {
            format: Some(arroyo_rpc::formats::Format::Json(arroyo_rpc::formats::JsonFormat {
                confluent_schema_registry: false,
                schema_id: None,
                include_schema: false,
                debezium: false,
                unstructured: false,
                timestamp_format: arroyo_rpc::formats::TimestampFormat::RFC3339,
            })),
            bad_data: None,
            framing: None,
            struct_name: Some("YellowstoneData".to_string()),
            fields: vec![],
            definition: Some(SchemaDefinition::JsonSchema(json_schema.to_string())),
            inferred: Some(true),
            primary_keys: Default::default(),
        })
    }

    fn test(
        &self,
        _: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        _: Option<&ConnectionSchema>,
        tx: Sender<TestSourceMessage>,
    ) {
        let connector = YellowstoneConnector::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let subscribe_config = match connector.parse_table_config(table) {
                Ok(config) => config,
                Err(e) => {
                    let _ = tx.send(TestSourceMessage {
                        error: true,
                        done: true,
                        message: format!("Failed to parse table config: {:?}", e),
                    }).await;
                    return;
                }
            };

            let endpoint = match config.endpoint.sub_env_vars() {
                Ok(endpoint) => endpoint.to_string(),
                Err(e) => {
                    let _ = tx.send(TestSourceMessage {
                        error: true,
                        done: true,
                        message: format!("Failed to resolve endpoint: {:?}", e),
                    }).await;
                    return;
                }
            };

            let x_token = if let Some(token) = config.x_token {
                match token.sub_env_vars() {
                    Ok(token) => Some(token.to_string()),
                    Err(e) => {
                        let _ = tx.send(TestSourceMessage {
                            error: true,
                            done: true,
                            message: format!("Failed to resolve x_token: {:?}", e),
                        }).await;
                        return;
                    }
                }
            } else {
                None
            };

            match connector.test_connection(endpoint, x_token).await {
                Ok(_) => {
                    let _ = tx.send(TestSourceMessage {
                        error: false,
                        done: true,
                        message: "Successfully connected to Yellowstone gRPC endpoint".to_string(),
                    }).await;
                },
                Err(e) => {
                    let _ = tx.send(TestSourceMessage {
                        error: true,
                        done: true,
                        message: format!("Failed to connect: {:?}", e),
                    }).await;
                }
            }
        });
    }

    fn from_config(
        &self,
        id: Option<i64>,
        name: &str,
        config: Self::ProfileT,
        table: Self::TableT,
        schema: Option<&ConnectionSchema>,
    ) -> anyhow::Result<Connection> {
        let _subscribe_config = self.parse_table_config(table.clone())?;
        
        let schema = self.get_schema(config.clone(), table.clone(), schema)
            .ok_or_else(|| anyhow!("Failed to get schema"))?;

        Ok(Connection {
            id,
            connector: self.name(),
            name: name.to_string(),
            connection_type: ConnectionType::Source,
            schema,
            config: serde_json::to_string(&table)?,
            description: format!("Yellowstone source reading from {}", config.endpoint.sub_env_vars().unwrap_or_default()),
            partition_fields: Some(vec![]),
        })
    }

    fn from_options(
        &self,
        name: &str,
        options: &mut ConnectorOptions,
        schema: Option<&ConnectionSchema>,
        _profile: Option<&ConnectionProfile>,
    ) -> anyhow::Result<Connection> {
        let config: YellowstoneProfile = options.config.take().unwrap();
        let table: YellowstoneTableConfig = options.table.take().unwrap();
        
        self.from_config(None, name, config, table, schema)
    }

    fn make_operator(
        &self,
        _: OperatorConfig,
        config: Self::ProfileT,
        table: Self::TableT,
        _: arroyo_rpc::OperatorMeta,
    ) -> anyhow::Result<ConstructedOperator> {
        let subscribe_config = self.parse_table_config(table)?;
        
        let endpoint = config.endpoint.sub_env_vars()
            .map_err(|e| anyhow!("Failed to resolve endpoint: {}", e))?
            .to_string();
            
        let x_token = if let Some(token) = config.x_token {
            Some(token.sub_env_vars()
                .map_err(|e| anyhow!("Failed to resolve x_token: {}", e))?
                .to_string())
        } else {
            None
        };
        
        Ok(ConstructedOperator::Source(Box::new(YellowstoneSourceFunc {
            endpoint,
            x_token,
            subscribe_config,
            format: arroyo_rpc::formats::Format::Json(arroyo_rpc::formats::JsonFormat {
                confluent_schema_registry: false,
                schema_id: None,
                include_schema: false,
                debezium: false,
                unstructured: false,
                timestamp_format: arroyo_rpc::formats::TimestampFormat::RFC3339,
            }),
            framing: None,
            bad_data: None,
            state: crate::yellowstone::source::YellowstoneSourceState::default(),
        })))
    }
}
