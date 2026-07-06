#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

function parseArgs(argv) {
  const args = new Map();
  for (let i = 0; i < argv.length; i += 1) {
    const value = argv[i];
    if (!value.startsWith("--")) {
      throw new Error(`unexpected positional argument: ${value}`);
    }
    const key = value.slice(2);
    const next = argv[i + 1];
    if (next === undefined || next.startsWith("--")) {
      args.set(key, "true");
    } else {
      args.set(key, next);
      i += 1;
    }
  }
  return args;
}

function requireArg(args, name) {
  const value = args.get(name);
  if (value === undefined || value.trim() === "") {
    throw new Error(`missing required argument --${name}`);
  }
  return value;
}

function optionalArg(args, name) {
  const value = args.get(name);
  return value === undefined || value.trim() === "" ? undefined : value;
}

function parseIntegerArg(args, name) {
  const raw = requireArg(args, name);
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`--${name} must be a positive integer`);
  }
  return parsed;
}

function matchesExportMail(text) {
  const normalized = text.toLowerCase();
  return (
    (normalized.includes("claude") || normalized.includes("anthropic")) &&
    normalized.includes("export")
  );
}

async function requirePlaywright() {
  try {
    return require("playwright");
  } catch (error) {
    throw new Error(
      `playwright module is required. Install it or set NODE_PATH to a node_modules directory that contains playwright. ${error.message}`,
    );
  }
}

async function ensureDirectory(directory) {
  await fs.mkdir(directory, { recursive: true });
}

async function launchContext(playwright, options) {
  await ensureDirectory(options.profileDir);
  await ensureDirectory(options.downloadDir);
  return playwright.chromium.launchPersistentContext(options.profileDir, {
    acceptDownloads: true,
    channel: "chrome",
    downloadsPath: options.downloadDir,
    headless: options.headless,
  });
}

async function openClaudeExportPanel(page, exportPeriod) {
  await page.goto("https://claude.ai/new", {
    waitUntil: "domcontentloaded",
    timeout: 60000,
  });
  if (/\/login|\/sign-in/.test(page.url())) {
    throw new Error("Claude is not authenticated in the selected browser profile");
  }

  const settingsButton = page.getByTestId("user-menu-button");
  await settingsButton.waitFor({ state: "visible", timeout: 30000 });
  await settingsButton.click();
  await page.getByTestId("user-menu-settings").click();
  await page.getByRole("button", { name: "Privacy" }).click();
  await page.getByRole("button", { name: "Export data" }).click();

  if (exportPeriod !== "All") {
    await page.getByRole("radio", { name: exportPeriod }).click();
  }
}

async function requestClaudeExport(page, exportPeriod) {
  await openClaudeExportPanel(page, exportPeriod);
  const responsePromise = page.waitForResponse(
    (response) =>
      response.url().includes("/api/organizations/") &&
      response.url().includes("/export_data"),
    { timeout: 60000 },
  );
  await page.getByRole("button", { name: "Export", exact: true }).click();
  const response = await responsePromise;
  const body = await response.text();
  if (response.status() !== 202) {
    throw new Error(`Claude export request failed: status=${response.status()} body=${body}`);
  }
  let parsed;
  try {
    parsed = JSON.parse(body);
  } catch (error) {
    throw new Error(`Claude export request returned non-JSON body: ${error.message}`);
  }
  if (typeof parsed.nonce !== "string" || parsed.nonce.trim() === "") {
    throw new Error(`Claude export request response did not include nonce: ${body}`);
  }
  return parsed;
}

async function searchGmail(page, query) {
  await page.goto("https://mail.google.com/mail/u/0/", {
    waitUntil: "domcontentloaded",
    timeout: 60000,
  });
  if (page.url().includes("accounts.google.com")) {
    throw new Error("Gmail is not authenticated in the selected browser profile");
  }
  const searchBox = page.getByRole("textbox", { name: /search mail/i });
  await searchBox.waitFor({ state: "visible", timeout: 60000 });
  await searchBox.fill(query);
  await page.keyboard.press("Enter");
  await page.waitForLoadState("domcontentloaded", { timeout: 60000 }).catch(() => {});
}

async function openLatestExportEmail(page, timeoutMs) {
  const query =
    '("Claude data export" OR "Your Claude data export" OR "data export") newer:1d';
  const deadline = Date.now() + timeoutMs;
  let searched = false;
  while (Date.now() < deadline) {
    if (!searched) {
      await searchGmail(page, query);
      searched = true;
    }
    await page.waitForTimeout(5000);
    const rows = await page.locator("tr").evaluateAll((items) =>
      items
        .map((row, index) => ({ index, text: row.innerText || "" }))
        .filter((row) => row.text),
    );
    const match = rows.find((row) => matchesExportMail(row.text));
    if (match) {
      await page.locator("tr").nth(match.index).click();
      await page.waitForLoadState("domcontentloaded", { timeout: 60000 }).catch(() => {});
      return;
    }
    await page.reload({ waitUntil: "domcontentloaded", timeout: 60000 }).catch(() => {});
  }
  throw new Error(`Claude export email was not found before timeout_ms=${timeoutMs}`);
}

async function downloadFromOpenedEmail(page, downloadDir) {
  const linkLocator = page.locator("a").filter({ hasText: /download|export|data/i });
  const count = await linkLocator.count();
  let candidate = undefined;
  for (let i = 0; i < count; i += 1) {
    const link = linkLocator.nth(i);
    const href = await link.getAttribute("href");
    if (href && (href.includes("claude.ai") || href.includes("anthropic"))) {
      candidate = link;
      break;
    }
  }
  if (!candidate) {
    const hrefs = await page.locator("a").evaluateAll((links) =>
      links.map((link) => ({ text: link.innerText || "", href: link.href || "" })),
    );
    throw new Error(`Claude export download link not found in email: ${JSON.stringify(hrefs)}`);
  }

  const downloadPromise = page.waitForEvent("download", { timeout: 120000 });
  await candidate.click();
  const download = await downloadPromise;
  const suggested = download.suggestedFilename();
  if (!suggested.toLowerCase().endsWith(".zip")) {
    throw new Error(`Claude export download did not produce a zip: ${suggested}`);
  }
  const target = path.join(downloadDir, suggested);
  await download.saveAs(target);
  return target;
}

async function downloadClaudeExport(page, downloadDir, timeoutMs) {
  await openLatestExportEmail(page, timeoutMs);
  return downloadFromOpenedEmail(page, downloadDir);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const mode = requireArg(args, "mode");
  const profileDir = path.resolve(requireArg(args, "profile-dir"));
  const downloadDir = path.resolve(requireArg(args, "download-dir"));
  const timeoutMs = parseIntegerArg(args, "timeout-ms");
  const exportPeriod = requireArg(args, "export-period");
  const headless = optionalArg(args, "headless") === "true";

  if (!["All", "30 days", "90 days"].includes(exportPeriod)) {
    throw new Error("--export-period must be one of: All, 30 days, 90 days");
  }
  if (!["request", "download", "request-and-download"].includes(mode)) {
    throw new Error("--mode must be request, download, or request-and-download");
  }

  const playwright = await requirePlaywright();
  const context = await launchContext(playwright, {
    profileDir,
    downloadDir,
    headless,
  });
  const page = await context.newPage();
  const startedAt = new Date().toISOString();
  try {
    const report = {
      status: "ok",
      mode,
      started_at: startedAt,
      finished_at: null,
      export_request: null,
      zip_path: null,
    };
    if (mode === "request" || mode === "request-and-download") {
      report.export_request = await requestClaudeExport(page, exportPeriod);
    }
    if (mode === "download" || mode === "request-and-download") {
      report.zip_path = await downloadClaudeExport(page, downloadDir, timeoutMs);
    }
    report.finished_at = new Date().toISOString();
    process.stdout.write(`${JSON.stringify(report)}\n`);
  } finally {
    await context.close();
  }
}

main().catch((error) => {
  const report = {
    status: "failed",
    error: error instanceof Error ? error.message : String(error),
    finished_at: new Date().toISOString(),
  };
  process.stderr.write(`${JSON.stringify(report)}\n`);
  process.exit(1);
});
