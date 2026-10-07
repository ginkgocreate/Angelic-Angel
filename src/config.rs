use crate::error::{Result, AngelicAngelError};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub twitter: TwitterConfig,
    pub registration: Option<Registration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TwitterConfig {
    pub auth_token: String,
    pub ct0: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebPushKeys {
    pub public_key: Vec<u8>,
    pub private_key: Vec<u8>,
    pub auth_secret: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoPushSession {
    pub uaid: String,
    pub channel_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Registration {
    pub endpoint: String,
    pub autopush: AutoPushSession,
    pub keys: WebPushKeys,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            AngelicAngelError::Config(format!("failed to read config ({}): {}", path.display(), e))
        })?;
        toml::from_str(&content).map_err(Into::into)
    }

    /// Writes the config atomically (temp file + rename) so a crash mid-write can't
    /// corrupt the stored registration. On Unix the file is created with mode 0600
    /// because it holds the Twitter session cookie and the push private key.
    pub fn save(&self, path: &Path) -> Result<()> {
        let content = toml::to_string_pretty(self)
            .map_err(|e| AngelicAngelError::Config(format!("failed to serialize config: {}", e)))?;

        let mut tmp = path.as_os_str().to_owned();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);

        write_private(&tmp, content.as_bytes())?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }
}

#[cfg(unix)]
fn write_private(path: &Path, content: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    // `mode` only applies on creation; tighten a pre-existing temp file too.
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    file.write_all(content)?;
    file.sync_all()
}

#[cfg(not(unix))]
fn write_private(path: &Path, content: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, content)
}

/// Shows only the length of a secret, e.g. `<40 chars>`.
pub fn mask_secret(secret: &str) -> String {
    format!("<{} chars>", secret.chars().count())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Config {
        Config {
            twitter: TwitterConfig {
                auth_token: "token".into(),
                ct0: "ct0".into(),
            },
            registration: Some(Registration {
                endpoint: "https://example.com/push".into(),
                autopush: AutoPushSession {
                    uaid: "uaid".into(),
                    channel_id: "chan".into(),
                },
                keys: WebPushKeys {
                    public_key: vec![4; 65],
                    private_key: vec![1; 32],
                    auth_secret: vec![2; 16],
                },
            }),
        }
    }

    #[test]
    fn save_and_load_roundtrip() {
        let dir = std::env::temp_dir().join(format!("aa-config-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("angelic-angel.toml");

        sample().save(&path).unwrap();
        // Overwrite works (rename over an existing file).
        sample().save(&path).unwrap();

        let loaded = Config::load(&path).unwrap();
        assert_eq!(loaded.twitter.auth_token, "token");
        assert_eq!(loaded.registration.unwrap().keys.private_key, vec![1; 32]);
        assert!(!dir.join("angelic-angel.toml.tmp").exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn mask_secret_hides_content() {
        assert_eq!(mask_secret("abcdef"), "<6 chars>");
    }
}
