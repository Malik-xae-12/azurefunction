"""
Load Orchestrator
─────────────────
Phase 1  → Cursor-paginate all deal IDs + properties  (100/page, sequential)
Phase 2  → Batch-fetch deal→contact & deal→company associations  (100 IDs/call, concurrent)
Phase 3  → Batch-fetch full contact + company records             (100 IDs/call, concurrent)
Phase 4  → Fetch attachments per deal                            (concurrent, semaphore-limited)
"""

import asyncio
import logging
from typing import Any, AsyncGenerator, Dict, List, Optional

from app.modules.hubspot.hubspot_client import HubSpotClient, _BATCH_SIZE
from app.modules.hubspot.schema import LoadProgress, LoadStatus

logger = logging.getLogger(__name__)

_CONCURRENCY = 10


def _chunks(lst: List, n: int):
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


class LoadOrchestrator:
    def __init__(self, client: HubSpotClient):
        self.client = client
        self._sem = asyncio.Semaphore(_CONCURRENCY)
        self._progress = LoadProgress(job_id="", status=LoadStatus.RUNNING)
        self._result_data: Optional[Dict[str, Any]] = None

    async def run(
        self,
        deal_properties: List[str],
        contact_properties: List[str],
        company_properties: List[str],
    ) -> AsyncGenerator[LoadProgress, None]:
        """Full initial load — yields progress after each phase."""
        try:
            # ── Phase 1: Paginate all deals ───────────────────────────────────
            all_deals: List[Dict] = []
            all_deal_ids: List[str] = []

            async for page_deals in self._paginate_deals(deal_properties):
                all_deals.extend(page_deals)
                all_deal_ids.extend(d["id"] for d in page_deals)
                self._progress.deals_fetched = len(all_deals)
                yield self._progress

            logger.info(f"Phase 1 complete: {len(all_deals)} deals fetched")

            # ── Phase 2: Batch-fetch associations ─────────────────────────────
            contact_assoc, company_assoc = await asyncio.gather(
                self._batch_associations("deals", "contacts", all_deal_ids),
                self._batch_associations("deals", "companies", all_deal_ids),
            )

            all_contact_ids = list({cid for ids in contact_assoc.values() for cid in ids})
            all_company_ids = list({cid for ids in company_assoc.values() for cid in ids})
            logger.info(f"Phase 2: {len(all_contact_ids)} contacts, {len(all_company_ids)} companies")
            yield self._progress

            # ── Phase 3: Batch-fetch full records ─────────────────────────────
            contacts_map, companies_map = await asyncio.gather(
                self._batch_fetch_objects("contacts", all_contact_ids, contact_properties),
                self._batch_fetch_objects("companies", all_company_ids, company_properties),
            )

            self._progress.contacts_fetched = len(contacts_map)
            self._progress.companies_fetched = len(companies_map)
            logger.info(f"Phase 3: {len(contacts_map)} contacts, {len(companies_map)} companies fetched")
            yield self._progress

            # ── Phase 4: Attachments per deal ─────────────────────────────────
            attachment_tasks = [self._fetch_deal_attachments(d["id"]) for d in all_deals]
            attachment_results = await asyncio.gather(*attachment_tasks, return_exceptions=True)

            total_attachments = 0
            attachments_map: Dict[str, List] = {}
            for deal, result in zip(all_deals, attachment_results):
                if isinstance(result, Exception):
                    logger.warning(f"Attachment fetch failed for deal {deal['id']}: {result}")
                    self._progress.errors.append(f"deal:{deal['id']} attachment error: {result}")
                else:
                    attachments_map[deal["id"]] = result
                    total_attachments += len(result)

            self._progress.attachments_fetched = total_attachments
            logger.info(f"Phase 4: {total_attachments} attachments")

            # ── Enrich + finish ────────────────────────────────────────────────
            enriched_deals = self._enrich_deals(
                deals=all_deals,
                contact_assoc=contact_assoc,
                company_assoc=company_assoc,
                contacts_map=contacts_map,
                companies_map=companies_map,
                attachments_map=attachments_map,
            )

            self._progress.result_sample = enriched_deals[:3]
            self._progress.status = LoadStatus.COMPLETED
            self._result_data = {
                "deals": all_deals,
                "contact_assoc": contact_assoc,
                "company_assoc": company_assoc,
                "contacts_map": contacts_map,
                "companies_map": companies_map,
                "attachments_map": attachments_map,
            }
            yield self._progress

        except Exception as exc:
            logger.exception("Orchestrator run failed")
            self._progress.status = LoadStatus.FAILED
            self._progress.error = str(exc)
            yield self._progress
            raise
        finally:
            await self.client.close()

    def get_result_data(self) -> Optional[Dict[str, Any]]:
        return self._result_data

    async def _paginate_deals(self, properties: List[str]) -> AsyncGenerator[List[Dict], None]:
        after: Optional[str] = None
        page = 0

        while True:
            data = await self.client.get_deals_page(after=after, properties=properties)
            results: List[Dict] = data.get("results", [])
            page += 1
            self._progress.pages_processed = page
            self._progress.api_calls_made += 1

            if not results:
                break

            yield results

            paging = data.get("paging", {})
            after = paging.get("next", {}).get("after")
            if not after:
                logger.info(f"Pagination complete after {page} pages")
                break

    async def _batch_associations(
        self, from_obj: str, to_obj: str, ids: List[str]
    ) -> Dict[str, List[str]]:
        assoc_map: Dict[str, List[str]] = {}
        chunks = list(_chunks(ids, _BATCH_SIZE))

        async def fetch_chunk(chunk: List[str]):
            async with self._sem:
                data = await self.client.batch_get_associations(from_obj, to_obj, chunk)
                self._progress.api_calls_made += 1
                for result in data.get("results", []):
                    from_id = result["from"]["id"]
                    to_ids = [r["id"] for r in result.get("to", [])]
                    assoc_map[from_id] = to_ids

        await asyncio.gather(*[fetch_chunk(c) for c in chunks])
        return assoc_map

    async def _batch_fetch_objects(
        self, object_type: str, ids: List[str], properties: List[str]
    ) -> Dict[str, Dict]:
        records_map: Dict[str, Dict] = {}
        chunks = list(_chunks(ids, _BATCH_SIZE))

        async def fetch_chunk(chunk: List[str]):
            async with self._sem:
                data = await self.client.batch_read_objects(object_type, chunk, properties)
                self._progress.api_calls_made += 1
                for record in data.get("results", []):
                    records_map[record["id"]] = record.get("properties", {})

        await asyncio.gather(*[fetch_chunk(c) for c in chunks])
        return records_map

    async def _fetch_deal_attachments(self, deal_id: str) -> List[Dict]:
        async with self._sem:
            data = await self.client.get_deal_attachments(deal_id)
            self._progress.api_calls_made += 1

        note_ids = [r["id"] for r in data.get("results", [])]
        if not note_ids:
            return []

        fetch_tasks = [self._fetch_note_attachments(nid, deal_id) for nid in note_ids[:50]]
        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        attachment_details = []
        for r in results:
            if not isinstance(r, Exception) and r:
                attachment_details.extend(r)
        return attachment_details

    async def _fetch_note_attachments(self, note_id: str, deal_id: str) -> List[Dict]:
        async with self._sem:
            data = await self.client.get_engagement_attachments(note_id)
            self._progress.api_calls_made += 1

        props = data.get("properties", {})
        attachment_ids_raw = props.get("hs_attachment_ids", "")
        if not attachment_ids_raw:
            return []

        attachment_ids = [a.strip() for a in attachment_ids_raw.split(";") if a.strip()]
        return [{"deal_id": deal_id, "note_id": note_id, "file_id": fid} for fid in attachment_ids]

    def _enrich_deals(
        self,
        deals: List[Dict],
        contact_assoc: Dict[str, List[str]],
        company_assoc: Dict[str, List[str]],
        contacts_map: Dict[str, Dict],
        companies_map: Dict[str, Dict],
        attachments_map: Dict[str, List],
    ) -> List[Dict]:
        enriched = []
        for deal in deals:
            did = deal["id"]
            enriched.append({
                "id": did,
                "properties": deal.get("properties", {}),
                "contacts": [
                    {"id": cid, "properties": contacts_map.get(cid, {})}
                    for cid in contact_assoc.get(did, [])
                ],
                "companies": [
                    {"id": cid, "properties": companies_map.get(cid, {})}
                    for cid in company_assoc.get(did, [])
                ],
                "attachments": attachments_map.get(did, []),
            })
        return enriched