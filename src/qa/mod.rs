use std::collections::HashMap;
use std::path::Path;

use chrono::{DateTime, NaiveDate, Utc};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

pub const READ_PERSONS_SCOPE: &str = "read:persons";
pub const READ_TIMELINE_SCOPE: &str = "read:timeline";
pub const ADMIN_SYNC_SCOPE: &str = "admin:sync";
pub const OPERATOR_ESCALATION_MESSAGE: &str =
    "この質問はチャットボットでは回答できません。重要事項は運営者に確認してください。";
pub const BUDGET_LIMIT_MESSAGE: &str =
    "現在応答を制限しています。重要事項は運営者に確認してください。";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QaPrincipal {
    pub name: String,
    pub scopes: Vec<String>,
}

impl QaPrincipal {
    pub fn validate_read_only(&self) -> Result<(), QaError> {
        if self.scopes.iter().any(|scope| scope == ADMIN_SYNC_SCOPE) {
            return Err(QaError::InvalidConfig(
                "Q&A token must not include admin:sync".to_string(),
            ));
        }
        for required in [READ_PERSONS_SCOPE, READ_TIMELINE_SCOPE] {
            if !self.scopes.iter().any(|scope| scope == required) {
                return Err(QaError::InvalidConfig(format!(
                    "Q&A token must include {required}"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RetrievalQuery {
    pub question: String,
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceContext {
    pub observation_id: String,
    pub human_label: String,
    pub text: String,
    pub confidence: f32,
}

pub trait Retriever {
    fn retrieve(&mut self, query: RetrievalQuery) -> Result<Vec<SourceContext>, QaError>;
}

#[derive(Debug, Clone)]
pub struct DailyCostCap {
    pub cap_usd: f64,
    pub spent_by_day: HashMap<NaiveDate, f64>,
}

impl DailyCostCap {
    fn assert_available(&self, day: NaiveDate) -> Result<(), QaError> {
        if self.cap_usd <= 0.0 {
            return Err(QaError::InvalidConfig(
                "daily cost cap must be positive".to_string(),
            ));
        }
        if self.spent_by_day.get(&day).copied().unwrap_or(0.0) >= self.cap_usd {
            return Err(QaError::BudgetExceeded);
        }
        Ok(())
    }

    fn record(&mut self, day: NaiveDate, cost_usd: f64) -> Result<(), QaError> {
        if cost_usd < 0.0 {
            return Err(QaError::InvalidConfig(
                "cost_usd must not be negative".to_string(),
            ));
        }
        *self.spent_by_day.entry(day).or_insert(0.0) += cost_usd;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QuestionAnswer {
    pub answer: String,
    pub provenance_ids: Vec<String>,
    pub escalated: bool,
    pub refused: bool,
    pub cost_usd: f64,
}

pub struct QaService<R: Retriever> {
    pub retriever: R,
    pub read_principal: QaPrincipal,
    pub log_store: AnswerLogStore,
    pub budget: DailyCostCap,
    pub model: String,
    pub operator_destination: String,
}

impl<R: Retriever> QaService<R> {
    pub fn new(
        retriever: R,
        read_principal: QaPrincipal,
        log_store: AnswerLogStore,
        budget: DailyCostCap,
        model: String,
        operator_destination: String,
    ) -> Result<Self, QaError> {
        read_principal.validate_read_only()?;
        if model.trim().is_empty() {
            return Err(QaError::InvalidConfig("model must be non-empty".to_string()));
        }
        if operator_destination.trim().is_empty() {
            return Err(QaError::InvalidConfig(
                "operator destination must be non-empty".to_string(),
            ));
        }
        Ok(Self {
            retriever,
            read_principal,
            log_store,
            budget,
            model,
            operator_destination,
        })
    }

    pub fn answer(
        &mut self,
        question: &str,
        now: DateTime<Utc>,
    ) -> Result<QuestionAnswer, QaError> {
        if question.trim().is_empty() {
            return Err(QaError::InvalidInput("question must be non-empty".to_string()));
        }

        let day = now.date_naive();
        match self.budget.assert_available(day) {
            Ok(()) => {}
            Err(QaError::BudgetExceeded) => {
                return self.record(
                    now,
                    question,
                    BUDGET_LIMIT_MESSAGE.to_string(),
                    Vec::new(),
                    true,
                    true,
                    0.0,
                );
            }
            Err(err) => return Err(err),
        }

        match classify_question(question) {
            QuestionClass::Refuse => {
                return self.record(
                    now,
                    question,
                    format!("{OPERATOR_ESCALATION_MESSAGE} 誘導先: {}", self.operator_destination),
                    Vec::new(),
                    true,
                    true,
                    0.0,
                );
            }
            QuestionClass::Escalate => {
                let contexts = self.retrieve(question)?;
                return self.record(
                    now,
                    question,
                    format!("{OPERATOR_ESCALATION_MESSAGE} 誘導先: {}", self.operator_destination),
                    contexts.into_iter().map(|context| context.observation_id).collect(),
                    true,
                    false,
                    0.0,
                );
            }
            QuestionClass::Answer => {}
        }

        let contexts = self.retrieve(question)?;
        let selected = contexts
            .into_iter()
            .filter(|context| context.confidence >= 0.5)
            .take(3)
            .collect::<Vec<_>>();
        if selected.is_empty() {
            return self.record(
                now,
                question,
                format!("{OPERATOR_ESCALATION_MESSAGE} 誘導先: {}", self.operator_destination),
                Vec::new(),
                true,
                false,
                0.0,
            );
        }

        let sources = selected
            .iter()
            .map(|context| format!("- {} ({})", context.human_label, context.observation_id))
            .collect::<Vec<_>>()
            .join("\n");
        let provenance_ids = selected
            .iter()
            .map(|context| context.observation_id.clone())
            .collect::<Vec<_>>();
        let answer = format!(
            "{}\n\n重要事項は運営者に確認してください。\n\n由来:\n{}",
            selected[0].text, sources
        );
        let cost_usd = estimate_cost_usd(question, &selected);
        self.budget.record(day, cost_usd)?;
        self.record(
            now,
            question,
            answer,
            provenance_ids,
            false,
            false,
            cost_usd,
        )
    }

    fn retrieve(&mut self, question: &str) -> Result<Vec<SourceContext>, QaError> {
        self.retriever.retrieve(RetrievalQuery {
            question: question.to_string(),
            scopes: vec![READ_PERSONS_SCOPE.to_string(), READ_TIMELINE_SCOPE.to_string()],
        })
    }

    fn record(
        &mut self,
        now: DateTime<Utc>,
        question: &str,
        answer: String,
        provenance_ids: Vec<String>,
        escalated: bool,
        refused: bool,
        cost_usd: f64,
    ) -> Result<QuestionAnswer, QaError> {
        self.log_store.append(
            now,
            question,
            &answer,
            &self.model,
            cost_usd,
            &provenance_ids,
        )?;
        Ok(QuestionAnswer {
            answer,
            provenance_ids,
            escalated,
            refused,
            cost_usd,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuestionClass {
    Answer,
    Refuse,
    Escalate,
}

pub fn classify_question(question: &str) -> QuestionClass {
    let refuse_keywords = [
        "個人間トラブル",
        "喧嘩",
        "ハラスメント",
        "健康相談",
        "病気",
        "契約",
        "返金",
        "支払い",
        "金銭",
    ];
    if refuse_keywords.iter().any(|keyword| question.contains(keyword)) {
        return QuestionClass::Refuse;
    }
    let escalate_keywords = [
        "本人確認",
        "誰が",
        "特定して",
        "全員の誕生日",
        "誕生日",
        "生年月日",
        "出身地",
        "連絡先",
        "メール",
        "電話番号",
    ];
    if escalate_keywords.iter().any(|keyword| question.contains(keyword)) {
        return QuestionClass::Escalate;
    }
    QuestionClass::Answer
}

fn estimate_cost_usd(question: &str, contexts: &[SourceContext]) -> f64 {
    let chars = question.chars().count()
        + contexts
            .iter()
            .map(|context| context.text.chars().count())
            .sum::<usize>();
    ((chars.max(1) as f64) * 0.000001 * 1_000_000.0).round() / 1_000_000.0
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AnswerLogEntry {
    pub id: i64,
    pub created_at: String,
    pub question: String,
    pub answer: String,
    pub model: String,
    pub cost_usd: f64,
    pub provenance_ids: Vec<String>,
    pub flagged: bool,
    pub flag_reason: Option<String>,
}

pub struct AnswerLogStore {
    connection: Connection,
}

impl AnswerLogStore {
    pub fn open(path: &Path) -> Result<Self, QaError> {
        let connection = Connection::open(path)?;
        let store = Self { connection };
        store.initialise()?;
        Ok(store)
    }

    pub fn append(
        &mut self,
        created_at: DateTime<Utc>,
        question: &str,
        answer: &str,
        model: &str,
        cost_usd: f64,
        provenance_ids: &[String],
    ) -> Result<i64, QaError> {
        self.connection.execute(
            "INSERT INTO qa_answers (created_at, question, answer, model, cost_usd, provenance_json, flagged, flag_reason) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, NULL)",
            params![
                created_at.to_rfc3339(),
                question,
                answer,
                model,
                cost_usd,
                serde_json::to_string(provenance_ids)?,
            ],
        )?;
        Ok(self.connection.last_insert_rowid())
    }

    pub fn recent(&self, limit: usize) -> Result<Vec<AnswerLogEntry>, QaError> {
        let mut statement = self.connection.prepare(
            "SELECT id, created_at, question, answer, model, cost_usd, provenance_json, flagged, flag_reason FROM qa_answers ORDER BY created_at DESC, id DESC LIMIT ?1",
        )?;
        let rows = statement.query_map(params![limit as i64], |row| {
            let provenance_json: String = row.get(6)?;
            let provenance_ids = serde_json::from_str::<Vec<String>>(&provenance_json)
                .map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        6,
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })?;
            Ok(AnswerLogEntry {
                id: row.get(0)?,
                created_at: row.get(1)?,
                question: row.get(2)?,
                answer: row.get(3)?,
                model: row.get(4)?,
                cost_usd: row.get(5)?,
                provenance_ids,
                flagged: row.get::<_, i64>(7)? != 0,
                flag_reason: row.get(8)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(QaError::from)
    }

    pub fn flag(&mut self, id: i64, reason: &str) -> Result<(), QaError> {
        if reason.trim().is_empty() {
            return Err(QaError::InvalidInput(
                "flag reason must be non-empty".to_string(),
            ));
        }
        let changed = self.connection.execute(
            "UPDATE qa_answers SET flagged = 1, flag_reason = ?1 WHERE id = ?2",
            params![reason.trim(), id],
        )?;
        if changed != 1 {
            return Err(QaError::NotFound(format!("answer log entry {id}")));
        }
        Ok(())
    }

    pub fn render_review_html(&self, limit: usize) -> Result<String, QaError> {
        let mut body = String::new();
        for entry in self.recent(limit)? {
            let flag_class = if entry.flagged { " flagged" } else { "" };
            let flag_reason = entry
                .flag_reason
                .as_deref()
                .map(|reason| format!("<p><strong>Flag:</strong> {}</p>", escape_html(reason)))
                .unwrap_or_default();
            body.push_str(&format!(
                "<article class='entry{flag_class}'><h2>#{}</h2><p><strong>Q:</strong> {}</p><p><strong>A:</strong> {}</p><p><strong>Sources:</strong> {}</p><p><strong>Model:</strong> {} / ${:.6}</p>{}</article>",
                entry.id,
                escape_html(&entry.question),
                escape_html(&entry.answer),
                escape_html(&entry.provenance_ids.join(", ")),
                escape_html(&entry.model),
                entry.cost_usd,
                flag_reason,
            ));
        }
        Ok(format!(
            "<!doctype html><html lang='ja'><head><meta charset='utf-8'><title>Q&A Review</title></head><body><h1>Q&A Review</h1>{body}</body></html>"
        ))
    }

    fn initialise(&self) -> Result<(), QaError> {
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS qa_answers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                question TEXT NOT NULL,
                answer TEXT NOT NULL,
                model TEXT NOT NULL,
                cost_usd REAL NOT NULL,
                provenance_json TEXT NOT NULL,
                flagged INTEGER NOT NULL,
                flag_reason TEXT
            )",
            [],
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct GoldenQuestion {
    pub question: String,
    pub expected: String,
}

pub fn load_golden_questions(raw: &str) -> Result<Vec<GoldenQuestion>, QaError> {
    let questions = serde_json::from_str::<Vec<GoldenQuestion>>(raw)?;
    validate_golden_questions(&questions)?;
    Ok(questions)
}

pub fn validate_golden_questions(questions: &[GoldenQuestion]) -> Result<(), QaError> {
    let count = |expected: &str| {
        questions
            .iter()
            .filter(|question| question.expected == expected)
            .count()
    };
    let answer = count("answer");
    let refuse = count("refuse");
    let escalate = count("escalate");
    if !(15..=20).contains(&answer) {
        return Err(QaError::InvalidInput(
            "golden set must include 15-20 answerable questions".to_string(),
        ));
    }
    if !(5..=10).contains(&refuse) {
        return Err(QaError::InvalidInput(
            "golden set must include 5-10 refused questions".to_string(),
        ));
    }
    if !(3..=5).contains(&escalate) {
        return Err(QaError::InvalidInput(
            "golden set must include 3-5 escalation questions".to_string(),
        ));
    }
    if questions
        .iter()
        .any(|question| !["answer", "refuse", "escalate"].contains(&question.expected.as_str()))
    {
        return Err(QaError::InvalidInput(
            "golden question expected must be answer/refuse/escalate".to_string(),
        ));
    }
    Ok(())
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[derive(Debug, thiserror::Error)]
pub enum QaError {
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("budget exceeded")]
    BudgetExceeded,
    #[error("not found: {0}")]
    NotFound(String),
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}
