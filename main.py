import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional
from dateutil import parser
from dateutil.relativedelta import relativedelta
import requests
from dotenv import load_dotenv

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ==========================================
# CONFIGURATION & CONSTANTS
# ==========================================
load_dotenv(override=True)

TIMEZONE = "America/Sao_Paulo"
LOG_FORMAT = "%(asctime)s - [%(levelname)s] - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

SCOPES = ['https://www.googleapis.com/auth/calendar.events']

# ==========================================
# RETRY DECORATORS
# ==========================================
retry_gcal = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type(Exception)
)

retry_notion = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type(Exception)
)

# ==========================================
# NOTION HTTP BYPASS
# ==========================================
@retry_notion
def fetch_notion_items(db_id: str, cursor: Optional[str] = None) -> Dict:
    """Busca itens no Notion via HTTP puro, filtrando apenas os recentes."""
    clean_db_id = db_id.strip().strip('"').strip("'")
    url = f"https://api.notion.com/v1/databases/{clean_db_id}/query"
    
    headers = {
        "Authorization": f"Bearer {os.getenv('NOTION_TOKEN')}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    
    # Filtro incremental: apenas itens modificados nos últimos 10 dias
    dez_dias_atras = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    
    # Adicionamos a tipagem explícita aqui: Dict[str, Any]
    body: Dict[str, Any] = {
        "filter": {
            "timestamp": "last_edited_time",
            "last_edited_time": {
                "on_or_after": dez_dias_atras
            }
        }
    }
    
    if cursor:
        body["start_cursor"] = cursor
        
    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()
    return response.json()

@retry_notion
def update_notion_page_gcal_id(page_id: str, gcal_id: str):
    """Atualiza o ID do GCal na página do Notion."""
    clean_page_id = page_id.strip()
    url = f"https://api.notion.com/v1/pages/{clean_page_id}"
    
    headers = {
        "Authorization": f"Bearer {os.getenv('NOTION_TOKEN')}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    
    body = {
        "properties": {
            "gcal_event_id": {  # Mude para "gcal" se você tiver mantido aquele ajuste!
                "rich_text": [{"text": {"content": gcal_id}}] if gcal_id else []
            }
        }
    }
    
    response = requests.patch(url, headers=headers, json=body)
    response.raise_for_status()

# ==========================================
# GOOGLE CALENDAR FUNCTIONS
# ==========================================
def extract_property_value(prop: Dict) -> Any:
    """Extrai valores das propriedades do Notion com segurança."""
    if not prop: return None
    ptype = prop.get("type")
    if ptype == "title": return "".join([t["plain_text"] for t in prop.get("title", [])])
    elif ptype == "rich_text": return "".join([t["plain_text"] for t in prop.get("rich_text", [])])
    elif ptype == "select":
        select = prop.get("select")
        return select.get("name") if select else None
    elif ptype == "url": return prop.get("url")
    elif ptype == "date": return prop.get("date")
    return None

def build_gcal_event_body(page_id: str, title: str, date_prop: Dict, url: str, link: str, origem: str) -> Dict:
    body = {
        "summary": title or "Sem Título",
        "description": f"Notion URL: {url}\nLink: {link or 'N/A'}\nOrigem: {origem or 'N/A'}",
        "extendedProperties": {"private": {"notion_page_id": page_id}}
    }

    start_str = date_prop.get("start")
    end_str = date_prop.get("end")

    if not isinstance(start_str, str):
        raise ValueError(f"Data inválida na página {page_id}")

    start_dt = parser.isoparse(start_str)
    
    if "T" not in start_str:
        body["start"] = {"date": start_dt.strftime("%Y-%m-%d"), "timeZone": TIMEZONE}
        end_dt = parser.isoparse(end_str) + relativedelta(days=1) if isinstance(end_str, str) else start_dt + relativedelta(days=1)
        body["end"] = {"date": end_dt.strftime("%Y-%m-%d"), "timeZone": TIMEZONE}
    else:
        body["start"] = {"dateTime": start_dt.isoformat(), "timeZone": TIMEZONE}
        end_dt = parser.isoparse(end_str) if isinstance(end_str, str) else start_dt + relativedelta(hours=1)
        body["end"] = {"dateTime": end_dt.isoformat(), "timeZone": TIMEZONE}

    return body

def events_differ(gcal_event: Dict, expected_body: Dict) -> bool:
    if gcal_event.get("summary", "") != expected_body.get("summary", ""): return True
    if gcal_event.get("description", "") != expected_body.get("description", ""): return True
    for key in ["start", "end"]:
        gcal_time = gcal_event.get(key, {})
        exp_time = expected_body.get(key, {})
        if gcal_time.get("date") != exp_time.get("date"): return True
        if gcal_time.get("dateTime") != exp_time.get("dateTime"): return True
    return False

@retry_gcal
def create_calendar_event(service, calendar_id: str, event_body: Dict) -> str:
    event = service.events().insert(calendarId=calendar_id, body=event_body).execute()
    return event['id']

@retry_gcal
def update_calendar_event(service, calendar_id: str, event_id: str, event_body: Dict):
    service.events().update(calendarId=calendar_id, eventId=event_id, body=event_body).execute()

@retry_gcal
def delete_calendar_event(service, calendar_id: str, event_id: str):
    try:
        service.events().delete(calendarId=calendar_id, eventId=event_id).execute()
    except HttpError as e:
        if e.resp.status != 404: raise e

@retry_gcal
def find_orphaned_event(service, calendar_id: str, notion_page_id: str) -> Optional[str]:
    events_result = service.events().list(
        calendarId=calendar_id, privateExtendedProperty=f"notion_page_id={notion_page_id}", maxResults=1
    ).execute()
    events = events_result.get('items', [])
    return events[0]['id'] if events else None

# ==========================================
# BUSINESS LOGIC
# ==========================================
def sync_page(page: Dict, gcal_service, calendar_id: str):
    page_id = page["id"]
    props = page["properties"]
    
    nome = extract_property_value(props.get("Nome"))
    data = extract_property_value(props.get("Data"))
    origem = extract_property_value(props.get("Origem"))
    link = extract_property_value(props.get("LINK"))
    gcal_event_id = extract_property_value(props.get("gcal_event_id")) # Adapte para "gcal" se necessário
    page_url = page["url"]

    if not data:
        if gcal_event_id:
            logger.info(f"Deletando evento GCal para a página {page_id} (Data removida).")
            delete_calendar_event(gcal_service, calendar_id, gcal_event_id)
            update_notion_page_gcal_id(page_id, "")
        return

    expected_body = build_gcal_event_body(page_id, nome, data, page_url, link, origem)

    if not gcal_event_id:
        orphaned_id = find_orphaned_event(gcal_service, calendar_id, page_id)
        if orphaned_id:
            logger.info(f"Recuperando evento órfão no GCal para {page_id}. Atualizando Notion.")
            update_notion_page_gcal_id(page_id, orphaned_id)
            gcal_event_id = orphaned_id
        else:
            logger.info(f"Criando novo evento GCal para {page_id}...")
            new_id = create_calendar_event(gcal_service, calendar_id, expected_body)
            update_notion_page_gcal_id(page_id, new_id)
            return

    if gcal_event_id:
        try:
            gcal_event = gcal_service.events().get(calendarId=calendar_id, eventId=gcal_event_id).execute()
            if events_differ(gcal_event, expected_body):
                logger.info(f"Atualizando evento GCal existente para {page_id}...")
                update_calendar_event(gcal_service, calendar_id, gcal_event_id, expected_body)
        except HttpError as e:
            if e.resp.status == 404:
                logger.warning(f"Evento {gcal_event_id} não encontrado no GCal. Recriando...")
                new_id = create_calendar_event(gcal_service, calendar_id, expected_body)
                update_notion_page_gcal_id(page_id, new_id)
            else:
                raise e

# ==========================================
# MAIN ROUTINE
# ==========================================
def main():
    logger.info("Iniciando sincronização Notion -> GCal...")
    
    NOTION_DB_ID = os.getenv("NOTION_DB_ID")
    GCAL_CALENDAR_ID = os.getenv("GCAL_CALENDAR_ID")
    GCAL_CREDENTIALS_PATH = os.getenv("GCAL_CREDENTIALS_PATH")

    if not all([os.getenv("NOTION_TOKEN"), NOTION_DB_ID, GCAL_CALENDAR_ID, GCAL_CREDENTIALS_PATH]):
        logger.error("Variáveis de ambiente ausentes. Verifique seu arquivo .env.")
        return

    try:
        credentials = service_account.Credentials.from_service_account_file(GCAL_CREDENTIALS_PATH, scopes=SCOPES) # type: ignore
        gcal_service = build('calendar', 'v3', credentials=credentials)
    except Exception as e:
        logger.error(f"Falha ao iniciar Google Client: {e}")
        return

    has_more = True
    next_cursor = None
    processed_count = 0

    while has_more:
        try:
            response = fetch_notion_items(NOTION_DB_ID, next_cursor) # type: ignore
            results = response.get("results", [])
            
            for page in results:
                try:
                    sync_page(page, gcal_service, GCAL_CALENDAR_ID) # type: ignore
                except Exception as e:
                    logger.error(f"Falha ao sincronizar página {page['id']}: {e}")

            processed_count += len(results)
            has_more = response.get("has_more")
            next_cursor = response.get("next_cursor")
            
        except Exception as e:
            logger.error(f"Erro crítico consultando Notion: {e}")
            break

    logger.info(f"Sincronização concluída. {processed_count} itens recentes processados.")

if __name__ == "__main__":
    main()