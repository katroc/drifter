
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WizardStep {
    Paths,
    Scanner,
    S3,
    Performance,
    Done,
}

#[derive(Debug, Clone)]
pub struct WizardState {
    pub step: WizardStep,
    pub field: usize,
    pub editing: bool,
    // Paths
    pub staging_dir: String,
    pub quarantine_dir: String,
    // Scanner
    pub clamd_host: String,
    pub clamd_port: String,
    // S3
    pub bucket: String,
    pub region: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    // Performance
    pub part_size: String,
    pub concurrency: String,
}

impl WizardState {
    pub fn new() -> Self {
        Self {
            step: WizardStep::Paths,
            field: 0,
            editing: false,
            staging_dir: "./staging".to_string(),
            quarantine_dir: "./quarantine".to_string(),
            clamd_host: "127.0.0.1".to_string(),
            clamd_port: "3310".to_string(),
            bucket: String::new(),
            region: "us-east-1".to_string(),
            endpoint: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            part_size: "64".to_string(),
            concurrency: "4".to_string(),
        }
    }

    pub fn field_count(&self) -> usize {
        match self.step {
            WizardStep::Paths => 2,
            WizardStep::Scanner => 2,
            WizardStep::S3 => 5,
            WizardStep::Performance => 2,
            WizardStep::Done => 0,
        }
    }

    pub fn get_field_mut(&mut self) -> Option<&mut String> {
        match (self.step, self.field) {
            (WizardStep::Paths, 0) => Some(&mut self.staging_dir),
            (WizardStep::Paths, 1) => Some(&mut self.quarantine_dir),
            (WizardStep::Scanner, 0) => Some(&mut self.clamd_host),
            (WizardStep::Scanner, 1) => Some(&mut self.clamd_port),
            (WizardStep::S3, 0) => Some(&mut self.bucket),
            (WizardStep::S3, 1) => Some(&mut self.region),
            (WizardStep::S3, 2) => Some(&mut self.endpoint),
            (WizardStep::S3, 3) => Some(&mut self.access_key),
            (WizardStep::S3, 4) => Some(&mut self.secret_key),
            (WizardStep::Performance, 0) => Some(&mut self.part_size),
            (WizardStep::Performance, 1) => Some(&mut self.concurrency),
            _ => None,
        }
    }

    pub fn next_step(&mut self) {
        self.step = match self.step {
            WizardStep::Paths => WizardStep::Scanner,
            WizardStep::Scanner => WizardStep::S3,
            WizardStep::S3 => WizardStep::Performance,
            WizardStep::Performance => WizardStep::Done,
            WizardStep::Done => WizardStep::Done,
        };
        self.field = 0;
    }

    pub fn prev_step(&mut self) {
        self.step = match self.step {
            WizardStep::Paths => WizardStep::Paths,
            WizardStep::Scanner => WizardStep::Paths,
            WizardStep::S3 => WizardStep::Scanner,
            WizardStep::Performance => WizardStep::S3,
            WizardStep::Done => WizardStep::Performance,
        };
        self.field = 0;
    }
}
