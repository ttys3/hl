// std imports
use std::collections::HashMap;
use std::include_str;

// third-party imports
use chrono_tz::Tz;
use config::{Config, File, FileFormat};
use derive_deref::Deref;
use platform_dirs::AppDirs;
use serde::{Deserialize, Serialize};

// local imports
use crate::error::Error;
use crate::types::Level;

// ---

static DEFAULT_SETTINGS: &str = include_str!("../etc/defaults/config.yaml");

// ---

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Settings {
    pub fields: Fields,
    pub concurrency: Option<usize>,
    pub time_format: String,
    pub time_zone: Tz,
    pub theme: String,
}

impl Settings {
    pub fn load(app_dirs: &AppDirs) -> Result<Self, Error> {
        let mut s = Config::default();
        let filename = app_dirs.config_dir.join("config.yaml");

        s.merge(File::from_str(DEFAULT_SETTINGS, FileFormat::Yaml))?;
        s.merge(File::with_name(&filename.to_string_lossy()).required(false))?;

        Ok(s.try_into()?)
    }
}

impl Default for Settings {
    fn default() -> Self {
        let mut s = Config::default();
        s.merge(File::from_str(DEFAULT_SETTINGS, FileFormat::Yaml))
            .unwrap();
        s.try_into().unwrap()
    }
}

// ---

#[derive(Debug, Serialize, Deserialize)]
pub struct Fields {
    pub predefined: PredefinedFields,
    pub ignore: Vec<String>,
    pub hide: Vec<String>,
}

// ---

#[derive(Debug, Serialize, Deserialize)]
pub struct PredefinedFields {
    pub time: TimeField,
    pub level: LevelField,
    pub message: MessageField,
    pub logger: LoggerField,
    pub caller: CallerField,
}

// ---

#[derive(Debug, Serialize, Deserialize, Deref)]
pub struct TimeField(pub Field);

// ---

#[derive(Debug, Serialize, Deserialize)]
pub struct LevelField {
    pub variants: Vec<LevelFieldVariant>,
}

// ---

#[derive(Debug, Serialize, Deserialize)]
pub struct LevelFieldVariant {
    pub names: Vec<String>,
    pub values: HashMap<Level, Vec<String>>,
}

// ---

#[derive(Debug, Serialize, Deserialize, Deref)]
pub struct MessageField(Field);

// ---

#[derive(Debug, Serialize, Deserialize, Deref)]
pub struct LoggerField(Field);

// ---

#[derive(Debug, Serialize, Deserialize, Deref)]
pub struct CallerField(Field);

// ---

#[derive(Debug, Serialize, Deserialize)]
pub struct Field {
    pub names: Vec<String>,
}

// ---
