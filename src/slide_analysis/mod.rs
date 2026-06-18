//! Slide Analysis Projector
//!
//! Analyses Google Slides observations from the lake, produces:
//! 1. SupplementalRecords with extracted student profile data
//! 2. WriteRecords for pushing to external SaaS (Notion)
//!
//! Ported from skcollege_dictionary pipeline (Main.js + AIService.js).

pub mod gemini;
pub mod projector;
pub mod provider;
pub mod types;

pub use gemini::GeminiSlideAnalyzer;
pub use projector::SlideAnalysisProjector;
pub use provider::{DerivationLineage, DerivationProvider, DerivedStudentProfile};
pub use types::*;
