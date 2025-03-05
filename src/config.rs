use std::{
    fmt,
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr},
    path::{Path, PathBuf},
};

use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize,
};

/// TOML files defining config values and topics
///
/// ```
/// ip = "0.0.0.0"
/// port = 8085
/// data_dir = "/tmp/franz-data"
///
/// [[topic]]
/// name = "test_topic_1"
/// page_size = "1 GiB"
/// max_pages = 16
///
/// [[topic]]
/// name = "other_topic"
/// page_size = 240_000
/// max_pages = 7
/// ````
#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    #[serde(default = "Config::default_ip")]
    pub ip: IpAddr,
    #[serde(default = "Config::default_port")]
    pub port: u16,
    #[serde(default = "Config::default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default = "Config::default_topic")]
    pub topic: Vec<Topic>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Topic {
    pub name: String,
    #[serde(deserialize_with = "byte_from_string")]
    pub page_size: u32,
    pub max_pages: usize,
}

impl Config {
    #[allow(dead_code)]
    pub fn create_example_config<P: AsRef<Path>>(path: P) -> Result<Config, ConfigError> {
        let mut f = std::fs::File::create_new(path.as_ref())?;
        let config = toml::from_str::<Config>("")?;
        f.write_all(
            toml::to_string(&config)
                .expect("that T wont 'decide' to fail")
                .as_bytes(),
        )?;

        f.write_all(
            br#"

# [[topic]]
# name = "example_topic"
# page_size = "1 GiB"
# max_pages = 16"#,
        )?;

        Ok(config)
    }

    fn default_data_dir() -> PathBuf {
        PathBuf::from("/tmp/franz-data")
    }

    fn default_ip() -> IpAddr {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    }

    fn default_port() -> u16 {
        8085
    }

    fn default_topic() -> Vec<Topic> {
        Vec::new()
    }
}

fn byte_from_string<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    // This is a Visitor that forwards string types to T's `FromStr` impl and
    // forwards map types to T's `Deserialize` impl. The `PhantomData` is to
    // keep the compiler from complaining about T being an unused generic type
    // parameter. We need T in order to know the Value type for the Visitor
    // impl.
    struct ByteFromString;

    impl Visitor<'_> for ByteFromString {
        type Value = u32;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("byte string")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(parse_size::parse_size(value).unwrap().try_into().unwrap())
        }
    }

    deserializer.deserialize_str(ByteFromString)
}

#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    Toml(#[from] toml::de::Error),
}

fn get_config_string<P: AsRef<Path>>(path: P) -> Result<String, ConfigError> {
    let mut config = String::new();

    if !path.as_ref().is_dir() {
        let Some(ext) = path.as_ref().extension() else {
            return Ok(config);
        };

        if ext != "toml" {
            return Ok(config);
        }

        let mut f = std::fs::File::open(path.as_ref())?;
        f.read_to_string(&mut config)?;

        return Ok(config);
    }

    for entry in path.as_ref().read_dir()? {
        let Ok(entry) = entry else {
            continue;
        };

        if let Ok(subconfig) = get_config_string(entry.path()) {
            config.push_str(&subconfig);
        }
    }

    Ok(config)
}

pub fn parse_config<P: AsRef<Path>>(path: P) -> Result<Config, ConfigError> {
    let config = get_config_string(path)?;
    Ok(toml::from_str(&config)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_config() {
        eprintln!(
            "{:#?}",
            toml::from_str::<Config>(
                r#"
            ip = "0.0.0.0"
            port = 8085

            [[topic]]
            name = "test_topic_1"
            page_size = "1 GiB"
            max_pages = 16

            [[topic]]
            name = "other_test_topic"
            page_size = "1024"
            max_pages = 2
            "#,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_config_defaults() {
        eprintln!("{:#?}", toml::from_str::<Config>(r#""#,).unwrap());
        let _ = Config::create_example_config("example.toml");
    }
}
