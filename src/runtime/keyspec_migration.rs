//! Blue/green keyspec migration state machine.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyspecVersionSet {
    pub routing_keyspec_version: String,
    pub identity_keyspec_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedKeyspecMetadata {
    pub routing_keyspec_version: String,
    pub identity_keyspec_version: String,
    pub partition_log_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationPhase {
    NewStructure,
    BulkRehomeModeB,
    IterativeCatchUp,
    Freeze,
    Cutover,
    Retired,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MigrationError {
    #[error("old and new keyspec versions must differ")]
    UnchangedKeyspec,
    #[error("migration phase out of order: expected {expected:?}, actual {actual:?}")]
    PhaseOutOfOrder {
        expected: MigrationPhase,
        actual: MigrationPhase,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlueGreenKeyspecMigration {
    old: KeyspecVersionSet,
    new: KeyspecVersionSet,
    new_partition_log_name: String,
    retained_old_metadata: RetainedKeyspecMetadata,
    phase: MigrationPhase,
}

impl BlueGreenKeyspecMigration {
    pub fn create(
        old: KeyspecVersionSet,
        new: KeyspecVersionSet,
        old_partition_log_name: String,
        new_partition_log_name: String,
    ) -> Result<Self, MigrationError> {
        if old == new {
            return Err(MigrationError::UnchangedKeyspec);
        }
        Ok(Self {
            retained_old_metadata: RetainedKeyspecMetadata {
                routing_keyspec_version: old.routing_keyspec_version.clone(),
                identity_keyspec_version: old.identity_keyspec_version.clone(),
                partition_log_name: old_partition_log_name,
            },
            old,
            new,
            new_partition_log_name,
            phase: MigrationPhase::NewStructure,
        })
    }

    pub fn record_bulk_rehome_mode_b(&mut self) -> Result<(), MigrationError> {
        self.advance(MigrationPhase::NewStructure, MigrationPhase::BulkRehomeModeB)
    }

    pub fn record_iterative_catch_up(&mut self) -> Result<(), MigrationError> {
        self.advance(MigrationPhase::BulkRehomeModeB, MigrationPhase::IterativeCatchUp)
    }

    pub fn freeze(&mut self) -> Result<(), MigrationError> {
        self.advance(MigrationPhase::IterativeCatchUp, MigrationPhase::Freeze)
    }

    pub fn cutover_reads(&mut self) -> Result<(), MigrationError> {
        self.advance(MigrationPhase::Freeze, MigrationPhase::Cutover)
    }

    pub fn retire_old_structure(&mut self) -> Result<(), MigrationError> {
        self.advance(MigrationPhase::Cutover, MigrationPhase::Retired)
    }

    pub fn phase(&self) -> MigrationPhase {
        self.phase
    }

    pub fn retained_old_metadata(&self) -> &RetainedKeyspecMetadata {
        &self.retained_old_metadata
    }

    pub fn new_partition_log_name(&self) -> &str {
        &self.new_partition_log_name
    }

    pub fn old_keyspecs(&self) -> &KeyspecVersionSet {
        &self.old
    }

    pub fn new_keyspecs(&self) -> &KeyspecVersionSet {
        &self.new
    }

    fn advance(
        &mut self,
        expected: MigrationPhase,
        next: MigrationPhase,
    ) -> Result<(), MigrationError> {
        if self.phase != expected {
            return Err(MigrationError::PhaseOutOfOrder {
                expected,
                actual: self.phase,
            });
        }
        self.phase = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keyspecs(suffix: &str) -> KeyspecVersionSet {
        KeyspecVersionSet {
            routing_keyspec_version: format!("routing-keyspec/{suffix}"),
            identity_keyspec_version: format!("identity-keyspec/{suffix}"),
        }
    }

    #[test]
    fn migration_rejects_in_place_keyspec_change() {
        let err = BlueGreenKeyspecMigration::create(
            keyspecs("v1"),
            keyspecs("v1"),
            "partition_log_old".to_owned(),
            "partition_log_new".to_owned(),
        )
        .unwrap_err();

        assert_eq!(err, MigrationError::UnchangedKeyspec);
    }

    #[test]
    fn migration_requires_bulk_catchup_freeze_cutover_retire_order() {
        let mut migration = BlueGreenKeyspecMigration::create(
            keyspecs("v1"),
            keyspecs("v2"),
            "partition_log_old".to_owned(),
            "partition_log_new".to_owned(),
        )
        .unwrap();

        assert!(matches!(
            migration.cutover_reads().unwrap_err(),
            MigrationError::PhaseOutOfOrder { .. }
        ));
        migration.record_bulk_rehome_mode_b().unwrap();
        migration.record_iterative_catch_up().unwrap();
        migration.freeze().unwrap();
        migration.cutover_reads().unwrap();
        migration.retire_old_structure().unwrap();

        assert_eq!(migration.phase(), MigrationPhase::Retired);
        assert_eq!(
            migration.retained_old_metadata(),
            &RetainedKeyspecMetadata {
                routing_keyspec_version: "routing-keyspec/v1".to_owned(),
                identity_keyspec_version: "identity-keyspec/v1".to_owned(),
                partition_log_name: "partition_log_old".to_owned(),
            }
        );
        assert_eq!(migration.new_partition_log_name(), "partition_log_new");
    }
}
