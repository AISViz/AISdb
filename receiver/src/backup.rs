use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BackupConfig {
    /// Whether to enable local backup
    pub enabled: bool,
    /// Directory to store backup files
    pub backup_dir: PathBuf,
    /// Maximum storage space in MB
    pub max_storage_mb: u64,
    /// File rotation interval in hours
    pub rotation_interval_hours: u64,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backup_dir: PathBuf::from("/home/pi/ais_backup"),
            max_storage_mb: 1000, // 1GB default
            rotation_interval_hours: 24,
        }
    }
}

pub struct BackupManager {
    config: BackupConfig,
    current_file: Option<File>,
    current_file_path: Option<PathBuf>,
    last_rotation: SystemTime,
}

impl BackupManager {
    pub fn new(config: BackupConfig) -> std::io::Result<Self> {
        if config.enabled {
            fs::create_dir_all(&config.backup_dir)?;
        }
        
        Ok(Self {
            config,
            current_file: None,
            current_file_path: None,
            last_rotation: SystemTime::now(),
        })
    }

    /// Write AIS data to backup file
    pub fn write_data(&mut self, data: &[u8]) -> std::io::Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        // Check if we need to rotate files
        if self.should_rotate() {
            self.rotate_files()?;
        }

        // Create or get current file
        let file = match &mut self.current_file {
            Some(f) => f,
            None => {
                let (file, path) = self.create_new_file()?;
                self.current_file = Some(file);
                self.current_file_path = Some(path);
                self.current_file.as_mut().unwrap()
            }
        };

        // Write data and append newline
        file.write_all(data)?;
        file.write_all(b"\n")?;
        file.flush()?;

        Ok(())
    }

    /// Check if we need to rotate files based on time or space constraints
    fn should_rotate(&self) -> bool {
        let elapsed = SystemTime::now()
            .duration_since(self.last_rotation)
            .unwrap_or_default();
        
        if elapsed.as_secs() >= self.config.rotation_interval_hours * 3600 {
            return true;
        }

        // Check total storage usage
        if let Ok(total_size) = self.get_total_storage_size() {
            if total_size > self.config.max_storage_mb * 1024 * 1024 {
                return true;
            }
        }

        false
    }

    /// Create a new backup file with timestamp
    fn create_new_file(&self) -> std::io::Result<(File, PathBuf)> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let filename = format!("ais_data_{}.txt", timestamp);
        let path = self.config.backup_dir.join(filename);
        
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
            
        Ok((file, path))
    }

    /// Rotate files and clean up old ones if needed
    fn rotate_files(&mut self) -> std::io::Result<()> {
        // Close current file if any
        self.current_file = None;
        self.current_file_path = None;
        
        // Get all backup files sorted by creation time
        let mut files: Vec<_> = fs::read_dir(&self.config.backup_dir)?
            .filter_map(|entry| entry.ok())
            .collect();
        
        files.sort_by_key(|entry| {
            entry.metadata()
                .and_then(|m| m.created())
                .unwrap_or(UNIX_EPOCH)
        });

        // Remove old files until we're under the storage limit
        while self.get_total_storage_size()? > self.config.max_storage_mb * 1024 * 1024 
            && !files.is_empty() {
            if let Some(oldest_file) = files.first() {
                fs::remove_file(oldest_file.path())?;
                files.remove(0);
            }
        }

        self.last_rotation = SystemTime::now();
        Ok(())
    }

    /// Calculate total size of backup files
    fn get_total_storage_size(&self) -> std::io::Result<u64> {
        let mut total_size = 0;
        for entry in fs::read_dir(&self.config.backup_dir)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            total_size += metadata.len();
        }
        Ok(total_size)
    }

    /// Get list of backup files with their creation times
    pub fn list_backup_files(&self) -> std::io::Result<Vec<(PathBuf, DateTime<Utc>)>> {
        let mut files = Vec::new();
        
        for entry in fs::read_dir(&self.config.backup_dir)? {
            let entry = entry?;
            let path = entry.path();
            let created = entry.metadata()?.created()?;
            let datetime: DateTime<Utc> = created.into();
            files.push((path, datetime));
        }
        
        files.sort_by_key(|(_path, time)| *time);
        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_backup_manager() -> std::io::Result<()> {
        let temp_dir = tempdir()?;
        
        let config = BackupConfig {
            enabled: true,
            backup_dir: temp_dir.path().to_path_buf(),
            max_storage_mb: 1,
            rotation_interval_hours: 1,
        };

        let mut manager = BackupManager::new(config)?;

        // Write some test data
        manager.write_data(b"Test data 1")?;
        manager.write_data(b"Test data 2")?;

        // Verify file was created
        let files = manager.list_backup_files()?;
        assert_eq!(files.len(), 1);

        // Force rotation by sleeping
        sleep(Duration::from_secs(1));
        manager.last_rotation = UNIX_EPOCH;
        manager.write_data(b"Test data 3")?;

        // Verify new file was created
        let files = manager.list_backup_files()?;
        assert_eq!(files.len(), 2);

        Ok(())
    }
}
