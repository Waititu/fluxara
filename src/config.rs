use std::{
    error::Error,
    fmt,
    net::{IpAddr, SocketAddr},
};

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 3000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub bind_addr: SocketAddr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConfigError {
    InvalidHost,
    InvalidPort,
    PortZero,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidHost => {
                write!(formatter, "FLUXARA_HOST must be a valid IP address")
            }
            Self::InvalidPort => {
                write!(formatter, "FLUXARA_PORT must be an integer from 1 to 65535")
            }
            Self::PortZero => write!(formatter, "FLUXARA_PORT must not be zero"),
        }
    }
}

impl Error for ConfigError {}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_variables(std::env::vars())
    }

    pub fn from_variables<I, K, V>(variables: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut host = DEFAULT_HOST.to_owned();
        let mut port = DEFAULT_PORT;

        for (key, value) in variables {
            match key.as_ref() {
                "FLUXARA_HOST" => host = value.as_ref().to_owned(),
                "FLUXARA_PORT" => {
                    port = value
                        .as_ref()
                        .parse::<u16>()
                        .map_err(|_| ConfigError::InvalidPort)?;
                }
                _ => {}
            }
        }

        if port == 0 {
            return Err(ConfigError::PortZero);
        }

        let host = host
            .parse::<IpAddr>()
            .map_err(|_| ConfigError::InvalidHost)?;

        Ok(Self {
            bind_addr: SocketAddr::new(host, port),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, ConfigError};
    use std::net::SocketAddr;

    #[test]
    fn defaults_to_loopback_port_3000() {
        let config = Config::from_variables(std::iter::empty::<(&str, &str)>()).unwrap();

        assert_eq!(
            config.bind_addr,
            "127.0.0.1:3000".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn accepts_valid_environment_values() {
        let config =
            Config::from_variables([("FLUXARA_HOST", "0.0.0.0"), ("FLUXARA_PORT", "8080")])
                .unwrap();

        assert_eq!(config.bind_addr.to_string(), "0.0.0.0:8080");
    }

    #[test]
    fn rejects_invalid_host() {
        let result = Config::from_variables([("FLUXARA_HOST", "not-an-ip")]);

        assert_eq!(result, Err(ConfigError::InvalidHost));
    }

    #[test]
    fn rejects_invalid_port() {
        let result = Config::from_variables([("FLUXARA_PORT", "not-a-port")]);

        assert_eq!(result, Err(ConfigError::InvalidPort));
    }

    #[test]
    fn rejects_zero_port() {
        let result = Config::from_variables([("FLUXARA_PORT", "0")]);

        assert_eq!(result, Err(ConfigError::PortZero));
    }
    #[test]
    fn rejects_port_above_u16_range() {
        let result = Config::from_variables([("FLUXARA_PORT", "65536")]);

        assert_eq!(result, Err(ConfigError::InvalidPort));
    }
}
