# HubSpot Integration - Complete Flow Explanation

## 🎯 Overview
Your application is a **4-Phase HubSpot Data Loader** that:
1. Fetches all deals from HubSpot
2. Gets associated contacts & companies for each deal
3. Fetches full details of all contacts & companies
4. Downloads attachments for each deal

---

## 📡 HubSpot APIs Used

### 1. **Get Deals (Paginated)**
```
GET /crm/v3/objects/deals?limit=100&archived=false&after={cursor}
```
- **Source**: HubSpot provides this official API
- **What it does**: Returns 100 deals per page with a cursor for next page
- **In your code**: `hubspot_client.py` → `get_deals_page()`
- **Why pagination**: HubSpot limits response to 100 items max, you need cursor-based pagination

### 2. **Batch Read Associations**
```
POST /crm/v3/associations/{fromObject}/{toObject}/batch/read
Body: {"inputs": [{"id": "deal_id_1"}, {"id": "deal_id_2"}, ...]}
```
- **Source**: HubSpot official API
- **What it does**: Gets up to 100 related object IDs in one call
- **In your code**: `hubspot_client.py` → `batch_get_associations()`
- **Examples**:
  - `/associations/deals/contacts/batch/read` → Get contact IDs linked to deals
  - `/associations/deals/companies/batch/read` → Get company IDs linked to deals

### 3. **Batch Read Objects (Full Records)**
```
POST /crm/v3/objects/{type}/batch/read
Body: {
  "inputs": [{"id": "contact_id_1"}, {"id": "contact_id_2"}, ...],
  "properties": ["firstname", "lastname", "email", "phone"]
}
```
- **Source**: HubSpot official API
- **What it does**: Gets full record details for up to 100 objects at once
- **In your code**: `hubspot_client.py` → `batch_read_objects()`
- **Examples**:
  - Get contact details (firstname, lastname, email, phone)
  - Get company details (name, domain, industry, city)

### 4. **Get Deal Associations (Notes/Attachments)**
```
GET /crm/v3/objects/deals/{dealId}/associations/notes
```
- **Source**: HubSpot official API
- **What it does**: Gets list of notes/engagement records linked to a deal
- **In your code**: `hubspot_client.py` → `get_deal_attachments()`

### 5. **Get Engagement (Note) Details**
```
GET /crm/v3/objects/notes/{engagementId}?properties=hs_attachment_ids,hs_note_body
```
- **Source**: HubSpot official API
- **What it does**: Gets attachment file IDs stored in a note
- **In your code**: `hubspot_client.py` → `get_engagement_attachments()`

---

## 🏗️ Architecture Layers

```
┌─────────────────────────────────────────────────────┐
│              FastAPI Router                         │
│  (router.py)                                        │
│  - /load/start     → Starts the process             │
│  - /load/status    → Checks job progress            │
│  - /load/result    → Gets final data                │
└──────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────────────────────────────────┐
│              Service Layer                          │
│  (service.py)                                       │
│  - start_load()     → Main orchestrator             │
│  - get_status()     → Progress tracker              │
│  - Database operations (insert/update)              │
└──────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────────────────────────────────┐
│          Load Orchestrator                          │
│  (load_orchestrator.py)                             │
│  - Phase 1: Paginate deals                          │
│  - Phase 2: Batch associations                      │
│  - Phase 3: Batch fetch full records                │
│  - Phase 4: Fetch attachments                       │
└──────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────────────────────────────────┐
│          HubSpot HTTP Client                        │
│  (hubspot_client.py)                                │
│  - Rate limiter (180 req/10 sec)                    │
│  - Retry logic (exponential backoff)                │
│  - Makes actual API calls                           │
└──────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────────────────────────────────┐
│          HubSpot Platform                           │
│  - CRM API endpoints                                │
└──────────────────────────────────────────────────────┘
```

---

## 🔄 Complete Flow Walkthrough

### **ENTRY POINT: POST /load/start**
```python
# router.py
@router.post("/load/start")
async def start_load(
    hubspot_token: str,  # Your API token
    deal_properties: str = "dealname,amount,dealstage,..."
    contact_properties: str = "firstname,lastname,email,phone"
    company_properties: str = "name,domain,industry,city"
)
```

### **PHASE 1: Paginate & Fetch All Deals**

**File**: `load_orchestrator.py` → `_paginate_deals()`

#### Step-by-step:
```
1. Start with after=null (first page)
2. Call HubSpot API: GET /crm/v3/objects/deals?limit=100&after=null
3. Get 100 deals back with properties (dealname, amount, dealstage, etc.)
4. Extract paging.next.after cursor from response
5. Repeat until no more pages

Example Response:
{
  "results": [
    {
      "id": "123",
      "properties": {
        "dealname": "Big Deal",
        "amount": "50000",
        "dealstage": "negotiation"
      }
    },
    ... (100 deals)
  ],
  "paging": {
    "next": {
      "after": "cursor_token_xyz"
    }
  }
}
```

**API Calls**: 1 per page (lets say you have 500 deals = 5 API calls)

**Progress Tracking**:
- `pages_processed` = incremented each page
- `deals_fetched` = total deals collected
- Stream progress updates to user

---

### **PHASE 2: Batch Fetch Associations (Contacts & Companies)**

**File**: `load_orchestrator.py` → `_batch_associations()`

#### Step-by-step:
```
1. Take all deal IDs from Phase 1 (e.g., [123, 456, 789, ...])
2. Split into chunks of 100 IDs
3. For each chunk, call TWO parallel APIs:

   API Call 1:
   POST /crm/v3/associations/deals/contacts/batch/read
   Body: {
     "inputs": [
       {"id": "123"}, {"id": "456"}, ... (100 IDs)
     ]
   }
   
   Response:
   {
     "results": [
       {
         "from": {"id": "deal_123"},
         "to": [
           {"id": "contact_456"},
           {"id": "contact_789"}
         ]
       },
       ...
     ]
   }

4. Do the same for companies:
   POST /crm/v3/associations/deals/companies/batch/read

5. Build maps:
   contact_assoc = {
     "deal_123": ["contact_456", "contact_789"],
     "deal_999": ["contact_111"]
   }
   
   company_assoc = {
     "deal_123": ["company_456"],
     "deal_999": ["company_111", "company_222"]
   }
```

**API Calls**: 2 calls per 100 deals (500 deals = 10 API calls)

**Concurrency**: Uses semaphore to limit to 10 concurrent requests

---

### **PHASE 3: Batch Fetch Full Records (Contact & Company Details)**

**File**: `load_orchestrator.py` → `_batch_fetch_objects()`

#### Step-by-step:
```
1. Collect all unique contact IDs from contact_assoc
   Example: [contact_456, contact_789, contact_111, ...]

2. Split into chunks of 100 IDs

3. For each chunk, call:
   POST /crm/v3/objects/contacts/batch/read
   Body: {
     "inputs": [
       {"id": "contact_456"},
       {"id": "contact_789"},
       ... (100 IDs)
     ],
     "properties": ["firstname", "lastname", "email", "phone"]
   }

   Response:
   {
     "results": [
       {
         "id": "contact_456",
         "properties": {
           "firstname": "John",
           "lastname": "Doe",
           "email": "john@example.com",
           "phone": "555-1234"
         }
       },
       ...
     ]
   }

4. Do the same for companies:
   POST /crm/v3/objects/companies/batch/read
   properties: ["name", "domain", "industry", "city"]

5. Build maps:
   contacts_map = {
     "contact_456": {
       "firstname": "John",
       "lastname": "Doe",
       "email": "john@example.com",
       "phone": "555-1234"
     },
     ...
   }
   
   companies_map = {
     "company_456": {
       "name": "Acme Inc",
       "domain": "acme.com",
       "industry": "Technology",
       "city": "San Francisco"
     },
     ...
   }
```

**API Calls**: 2 calls per ~100 unique contacts/companies

---

### **PHASE 4: Fetch Attachments**

**File**: `load_orchestrator.py` → `_fetch_deal_attachments()` & `_fetch_note_attachments()`

#### Step-by-step:
```
1. For EACH deal (concurrent, max 10 at a time):

   GET /crm/v3/objects/deals/{deal_id}/associations/notes
   
   Response:
   {
     "results": [
       {"id": "note_111"},
       {"id": "note_222"},
       {"id": "note_333"}
     ]
   }

2. For each note found (up to 50 per deal):

   GET /crm/v3/objects/notes/{note_id}?properties=hs_attachment_ids
   
   Response:
   {
     "id": "note_111",
     "properties": {
       "hs_attachment_ids": "file_abc;file_def;file_ghi"
     }
   }

3. Extract file IDs from hs_attachment_ids (semicolon-separated string)

4. Build attachments map:
   attachments_map = {
     "deal_123": [
       {"deal_id": "deal_123", "note_id": "note_111", "file_id": "file_abc"},
       {"deal_id": "deal_123", "note_id": "note_111", "file_id": "file_def"},
       ...
     ],
     ...
   }
```

**API Calls**: 1 per deal + 1 per note (can be many!)

**Concurrency**: Semaphore limits to 10 concurrent requests

---

## 🛡️ Safety Features

### **1. Rate Limiting** (`hubspot_client.py`)
```python
class RateLimiter:
    - Allows 180 requests per 10 seconds
    - Uses token-bucket algorithm
    - Sleeps automatically if limit reached
    - Prevents API throttling
```

### **2. Retry Logic** (`hubspot_client.py`)
```python
For MAX 5 attempts:
  - 429 (rate-limited): Sleep for Retry-After header
  - 500+ (server error): Exponential backoff (2^attempt seconds)
  - Network error: Exponential backoff
```

### **3. Error Tracking** (`load_orchestrator.py`)
```python
progress.errors = []  # List of error messages
progress.error = None # Overall error if everything fails

When Phase 4 (attachments) fails for a deal:
  - Log the error
  - Add to errors list
  - Continue processing other deals
  - Don't crash entire process
```

---

## 💾 Data Persistence (`service.py`)

### **During Processing** (Streaming)
```
1. Create Job record in database → job_id, status=RUNNING
2. After each phase, update Job record with progress
3. Stream JSON to user in real-time via SSE (Server-Sent Events)
```

### **On Completion**
```python
persist_load_data() calls:
  ├─ _upsert_deals(deals)
  ├─ _upsert_contacts(contacts_map)
  ├─ _upsert_companies(companies_map)
  ├─ _replace_deal_contacts(contact_assoc)
  ├─ _replace_deal_companies(company_assoc)
  └─ _replace_attachments(attachments_map)

All in ONE transaction → atomicity
```

---

## 📊 Example Full Flow for 500 Deals

```
API CALL BREAKDOWN:
┌─────────────────────────────────────────────────────┐
│ Phase 1: Paginate Deals                             │
│ ➜ 5 calls (500 deals ÷ 100 per page)               │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│ Phase 2: Batch Associations                         │
│ ➜ 10 calls (5 chunks × 2 APIs: contacts + companies)│
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│ Phase 3: Batch Fetch Records                        │
│ ➜ ~4 calls for contacts + ~2 calls for companies    │
│   (depends on unique count)                         │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│ Phase 4: Attachments                                │
│ ➜ ~500 calls for deals + ~1500 calls for notes      │
│   (if avg 3 notes per deal)                         │
└─────────────────────────────────────────────────────┘

TOTAL: ~2,000 API calls for 500 deals
WITH RATE LIMITING: 180 req/10s = ~111 seconds
```

---

## 🎬 Streaming Response to User

```python
return StreamingResponse(
    event_generator(),
    media_type="text/event-stream"
)
```

**How it works**:
```
Browser/Postman connects → Opens persistent HTTP connection
   ↓
As each phase completes → Server sends: "data: {progress_json}\n\n"
   ↓
Browser receives updates → Updates UI in real-time
   ↓
On completion → Connection closes

Example SSE Output:
data: {"job_id":"load_20250317_120000","status":"running","deals_fetched":100,...}

data: {"job_id":"load_20250317_120000","status":"running","deals_fetched":200,...}

data: {"job_id":"load_20250317_120000","status":"completed",...}
```

---

## ✅ How Errors Are Handled

### **Attachment Phase Failures** (Graceful)
```python
attachment_tasks = [fetch for each deal]
results = await asyncio.gather(*attachment_tasks, return_exceptions=True)

for deal, result in zip(all_deals, results):
    if isinstance(result, Exception):
        # Log error but continue
        progress.errors.append(f"deal:{deal_id} error: {result}")
    else:
        # Success - add to map
        attachments_map[deal_id] = result
```
→ **One deal's attachment failure doesn't stop the entire process**

### **Critical Failures** (Stop Everything)
```python
try:
    # Full Phase 1-4 process
except Exception as exc:
    progress.status = LoadStatus.FAILED
    progress.error = str(exc)
    db.commit()  # Save error state
    yield progress
    raise  # Re-raise to stop
```
→ **If Phase 1-3 fail, entire process stops and saves error**

---

## 🚀 Batch Read Explanation (Your Key Question!)

### **Is Batch Read Provided by HubSpot?**
**YES!** HubSpot provides official batch APIs:

```
Official HubSpot Batch APIs:
- /crm/v3/associations/{from}/{to}/batch/read     ✅ You use this
- /crm/v3/objects/{type}/batch/read                ✅ You use this
```

### **Why 100 Items Per Batch?**
- HubSpot's API limit is **100 objects max per batch request**
- You chunk your IDs into groups of 100
- Then make multiple parallel requests

### **Why Batch Instead of Single?**
```
WITHOUT Batch (fetching 500 contacts individually):
  500 API calls × 1 contact each = SLOW + hits rate limit

WITH Batch (fetching 500 contacts in batches):
  5 API calls × 100 contacts each = FAST + efficient
```

---

## 📋 Function Purpose Summary

| Function | File | Purpose | HubSpot API Used |
|----------|------|---------|------------------|
| `start_load()` | service.py | Main entry point, creates job, starts streaming | None (orchestrator) |
| `get_status()` | service.py | Returns job progress from memory/DB | None |
| `get_result()` | service.py | Returns final results | None |
| `run()` | load_orchestrator.py | 4-phase orchestration | All phases |
| `_paginate_deals()` | load_orchestrator.py | Phase 1: Get all deals with cursor | GET /crm/v3/objects/deals |
| `_batch_associations()` | load_orchestrator.py | Phase 2: Get contact/company IDs | POST /associations/{from}/{to}/batch/read |
| `_batch_fetch_objects()` | load_orchestrator.py | Phase 3: Get full record details | POST /crm/v3/objects/{type}/batch/read |
| `_fetch_deal_attachments()` | load_orchestrator.py | Phase 4: Get notes per deal | GET /crm/v3/objects/deals/{id}/associations/notes |
| `_fetch_note_attachments()` | load_orchestrator.py | Phase 4: Get attachments per note | GET /crm/v3/objects/notes/{id} |
| `_enrich_deals()` | load_orchestrator.py | Combine all data into unified objects | None |
| `_request()` | hubspot_client.py | HTTP wrapper with rate limit/retry | Makes actual API calls |
| `get_deals_page()` | hubspot_client.py | Wrapper for deals pagination | GET /crm/v3/objects/deals |
| `batch_get_associations()` | hubspot_client.py | Wrapper for associations batch | POST /crm/v3/associations/.../batch/read |
| `batch_read_objects()` | hubspot_client.py | Wrapper for objects batch read | POST /crm/v3/objects/{type}/batch/read |
| `get_deal_attachments()` | hubspot_client.py | Wrapper for deal notes | GET /crm/v3/objects/deals/{id}/associations/notes |
| `get_engagement_attachments()` | hubspot_client.py | Wrapper for note details | GET /crm/v3/objects/notes/{id} |
| `persist_load_data()` | service.py | Save all data to database | None (DB operation) |

---

## 🎯 Key Takeaways

1. **HubSpot provides official batch APIs** - Not custom, you're using their API properly
2. **100 items per batch** - HubSpot's documented limit
3. **4-phase architecture** - Sequential phases with real-time streaming
4. **Rate limiting** - Built-in (180 req/10s) to avoid throttling
5. **Error handling** - Graceful for attachments, strict for main phases
6. **Streaming response** - Real-time updates to user via SSE
7. **Atomic persistence** - All database operations in single transaction
8. **Concurrent requests** - Semaphore limits parallelism (10 concurrent max)

---
