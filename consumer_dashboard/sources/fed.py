"""Federal Reserve Board source adapter."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
import hashlib
import io
import re
import zipfile

import httpx

from consumer_dashboard import __version__
from consumer_dashboard.config.registry import SourceDefinition
from consumer_dashboard.sources.base import AcquisitionResult, BaseSourceAdapter
from consumer_dashboard.storage.filesystem import ensure_directory, write_json

G19_HISTORICAL_LEVELS_URL = "https://www.federalreserve.gov/releases/g19/HIST/cc_hist_sa_levels.html"
Z1_RELEASE_PAGE_URL = "https://www.federalreserve.gov/releases/z1/"
Z1_RELEASE_DATE_PATTERN = re.compile(r"Release Date:\s*(?P<release_date>[A-Za-z]+ \d{1,2}, \d{4})", re.IGNORECASE)
Z1_CSV_ZIP_PATTERN = re.compile(r'href="(?P<csv_path>/releases/z1/\d{8}/z1_csv_files\.zip)"', re.IGNORECASE)
LAST_UPDATE_PATTERN = re.compile(r"Last Update:\s*(?P<release_date>[A-Za-z]+ \d{1,2}, \d{4})", re.IGNORECASE)

# DFA – Distributional Financial Accounts
DFA_RELEASE_PAGE_URL = "https://www.federalreserve.gov/releases/efa/efa-distributional-financial-accounts.htm"
DFA_CSV_ZIP_PATTERN = re.compile(
    r'href="(?P<csv_path>/releases/efa/\d{8}/efa_csv_files\.zip)"',
    re.IGNORECASE,
)
DFA_RELEASE_DATE_PATTERN = re.compile(
    r"Release Date:\s*(?P<release_date>[A-Za-z]+ \d{1,2}, \d{4})",
    re.IGNORECASE,
)


def _parse_release_date(value: str) -> str:
    return datetime.strptime(value, "%B %d, %Y").date().isoformat()


def _extract_last_update(page_text: str) -> str | None:
    match = LAST_UPDATE_PATTERN.search(page_text)
    if not match:
        return None
    return _parse_release_date(match.group("release_date"))


def _extract_z1_release_metadata(page_text: str) -> dict[str, str] | None:
    release_match = Z1_RELEASE_DATE_PATTERN.search(page_text)
    csv_match = Z1_CSV_ZIP_PATTERN.search(page_text)
    if not release_match or not csv_match:
        return None
    csv_path = csv_match.group("csv_path")
    return {
        "release_date": _parse_release_date(release_match.group("release_date")),
        "csv_path": csv_path,
        "csv_url": f"https://www.federalreserve.gov{csv_path}",
    }


def _extract_zip_member_text(zip_bytes: bytes, member_path: str) -> str:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        with archive.open(member_path) as handle:
            return handle.read().decode("utf-8-sig")


class FederalReserveBoardSourceAdapter(BaseSourceAdapter):
    source_id = "federal_reserve_board"

    def fetch_latest(self, definition: SourceDefinition) -> AcquisitionResult:
        fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        artifact_dir = ensure_directory(self.settings.raw_dir / self.source_id / run_id)
        g19_artifact_path = artifact_dir / "g19_consumer_credit_levels.json"
        z1_artifact_path = artifact_dir / "z1_household_balance_sheet.json"
        dfa_artifact_path = artifact_dir / "dfa_distributional.json"

        try:
            with httpx.Client(timeout=self.settings.http_timeout_seconds) as client:
                g19_response = client.get(G19_HISTORICAL_LEVELS_URL)
                g19_response.raise_for_status()
                g19_page = g19_response.text

                z1_page_response = client.get(Z1_RELEASE_PAGE_URL)
                z1_page_response.raise_for_status()
                z1_page = z1_page_response.text

                z1_metadata = _extract_z1_release_metadata(z1_page)
                if z1_metadata is None:
                    return AcquisitionResult(
                        source_id=self.source_id,
                        status="parse_failed",
                        message="Unable to identify the current Z.1 CSV download from the Federal Reserve release page.",
                    )

                z1_zip_response = client.get(z1_metadata["csv_url"])
                z1_zip_response.raise_for_status()
                z1_zip_bytes = z1_zip_response.content

                # DFA fetch — best-effort; failure does not block the main Fed artifacts
                dfa_page_text = ""
                dfa_zip_bytes = b""
                dfa_release_date = ""
                try:
                    dfa_page_response = client.get(DFA_RELEASE_PAGE_URL)
                    dfa_page_response.raise_for_status()
                    dfa_page_text = dfa_page_response.text
                    dfa_csv_match = DFA_CSV_ZIP_PATTERN.search(dfa_page_text)
                    date_match = DFA_RELEASE_DATE_PATTERN.search(dfa_page_text)
                    if dfa_csv_match:
                        dfa_zip_url = f"https://www.federalreserve.gov{dfa_csv_match.group('csv_path')}"
                        dfa_zip_resp = client.get(dfa_zip_url)
                        dfa_zip_resp.raise_for_status()
                        dfa_zip_bytes = dfa_zip_resp.content
                    if date_match:
                        dfa_release_date = _parse_release_date(date_match.group("release_date"))
                except httpx.HTTPError:
                    pass  # DFA is best-effort; do not fail the whole ingest

        except httpx.HTTPError as exc:
            return AcquisitionResult(
                source_id=self.source_id,
                status="request_failed",
                message=f"Federal Reserve Board request failed: {exc}",
            )

        g19_release_date = _extract_last_update(g19_page) or fetched_at
        g19_envelope = {
            "metadata": {
                "source_id": self.source_id,
                "source_name": definition.source_name,
                "report_slug": "consumer_credit_g19",
                "report_name": "Consumer Credit - G.19",
                "fetched_at": fetched_at,
                "release_date": g19_release_date,
                "artifact_type": "html_page",
                "adapter_version": __version__,
                "endpoint": G19_HISTORICAL_LEVELS_URL,
                "http_status": g19_response.status_code,
                "response_sha256": hashlib.sha256(g19_page.encode("utf-8")).hexdigest(),
            },
            "response_text": g19_page,
        }
        write_json(g19_artifact_path, g19_envelope)

        z1_csv_text = _extract_zip_member_text(z1_zip_bytes, "csv/b101e.csv")
        z1_data_dictionary = _extract_zip_member_text(z1_zip_bytes, "data_dictionary/b101e.txt")
        z1_envelope = {
            "metadata": {
                "source_id": self.source_id,
                "source_name": definition.source_name,
                "report_slug": "financial_accounts_z1",
                "report_name": "Financial Accounts of the United States - Z.1",
                "fetched_at": fetched_at,
                "release_date": z1_metadata["release_date"],
                "artifact_type": "csv_extract",
                "adapter_version": __version__,
                "endpoint": z1_metadata["csv_url"],
                "http_status": z1_zip_response.status_code,
                "zip_sha256": hashlib.sha256(z1_zip_bytes).hexdigest(),
                "source_table_name": "B.101.e Balance Sheet of Households and Nonprofit Organizations with Debt and Equity Holdings Detail",
                "csv_member_path": "csv/b101e.csv",
                "dictionary_member_path": "data_dictionary/b101e.txt",
            },
            "csv_text": z1_csv_text,
            "data_dictionary": z1_data_dictionary,
        }
        write_json(z1_artifact_path, z1_envelope)

        # DFA artifact — store whatever was fetched (may be empty if page structure changed)
        dfa_csv_texts: dict[str, str] = {}
        if dfa_zip_bytes:
            try:
                dfa_csv_targets = [
                    "csv/dfa-level-detail.csv",
                    "csv/dfa-shares.csv",
                ]
                for target in dfa_csv_targets:
                    try:
                        dfa_csv_texts[target] = _extract_zip_member_text(dfa_zip_bytes, target)
                    except KeyError:
                        pass
            except Exception:
                pass

        dfa_envelope = {
            "metadata": {
                "source_id": self.source_id,
                "source_name": definition.source_name,
                "report_slug": "distributional_financial_accounts",
                "report_name": "Distributional Financial Accounts (DFA)",
                "fetched_at": fetched_at,
                "release_date": dfa_release_date or fetched_at,
                "artifact_type": "csv_extract",
                "adapter_version": __version__,
                "endpoint": DFA_RELEASE_PAGE_URL,
                "zip_sha256": hashlib.sha256(dfa_zip_bytes).hexdigest() if dfa_zip_bytes else "",
                "csv_files_extracted": list(dfa_csv_texts.keys()),
            },
            "csv_texts": dfa_csv_texts,
        }
        write_json(dfa_artifact_path, dfa_envelope)

        z1_rows = max(sum(1 for _ in csv.DictReader(io.StringIO(z1_csv_text))), 0)
        dfa_status = f", DFA distributional ({len(dfa_csv_texts)} CSV files)" if dfa_csv_texts else " (DFA not available)"
        return AcquisitionResult(
            source_id=self.source_id,
            status="ingested",
            message=(
                "Downloaded Federal Reserve Board G.19 consumer credit history, "
                f"Z.1 household balance-sheet data through {z1_metadata['release_date']} "
                f"({z1_rows} quarterly rows in B.101.e){dfa_status}."
            ),
            artifacts=[str(g19_artifact_path), str(z1_artifact_path), str(dfa_artifact_path)],
        )
