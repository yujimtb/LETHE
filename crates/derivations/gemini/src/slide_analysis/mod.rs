//! Slide Analysis Projector
//!
//! Analyses Google Slides observations from the lake, produces:
//! 1. SupplementalRecords with extracted student profile data

pub mod gemini;
pub mod projector;
pub mod provider;

pub use gemini::GeminiSlideAnalyzer;
pub use lethe_profile_model::*;
pub use projector::SlideAnalysisProjector;
pub use provider::{DerivationLineage, DerivationProvider, DerivedStudentProfile};
