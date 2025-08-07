import re
import structlog
from crawler.database.connection import get_session
from crawler.database.models import Location

logger = structlog.get_logger(__name__)

def clean_address(address_detail: str) -> str:
    """
    Cleans the address detail string by:
    1. Removing text after the first '/' character.
    2. Removing any text enclosed in parentheses '()'.
    """
    if not address_detail:
        return address_detail

    # Remove any text enclosed in parentheses '()'
    cleaned_address = re.sub(r'[\(（][^）)]*[\)）]', '', address_detail)

    # Remove any text enclosed in square brackets '[]'
    cleaned_address = re.sub(r'\[.*?\]', '', cleaned_address)

    # Remove text after the first '~'
    cleaned_address = cleaned_address.split('~', 1)[0]

    # Remove text after the first '.'
    cleaned_address = cleaned_address.split('.', 1)[0]

    # Remove text after the first '/'
    cleaned_address = cleaned_address.split('/', 1)[0]

    return cleaned_address.strip()

def main():
    logger.info("Starting address detail cleaning process...")
    updated_count = 0
    with get_session() as session:
        locations = session.query(Location).all()
        for location in locations:
            original_address = location.address_detail
            cleaned_address = clean_address(original_address)
            
            if original_address != cleaned_address:
                location.address_detail = cleaned_address
                updated_count += 1
                logger.debug(f"Cleaned: '{original_address}' -> '{cleaned_address}'")
        
        session.commit()
    logger.info(f"Address detail cleaning process completed. Updated {updated_count} records.")

if __name__ == "__main__":
    main()
