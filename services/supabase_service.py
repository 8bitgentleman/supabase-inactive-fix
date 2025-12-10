# services/supabase_service.py

import logging
from supabase import create_client, Client


class SupabaseClient:
    def __init__(self, url, key, table_name):
        if not url or not key:
            raise ValueError("Supabase URL and Key must be provided.")

        self.client: Client = create_client(url, key)
        self.table_name = table_name

    def insert_random_name(self, random_name):
        data = {'name': random_name}
        try:
            response = self.client.table(self.table_name).insert(data).execute()
            logging.info(f"✓ Successfully inserted entry into '{self.table_name}' (REST API)")
            logging.debug(f"Response data: {response.data}")
            return True
        except Exception as e:
            logging.error(f"✗ Error inserting data into '{self.table_name}': {e}")
            return False

    def get_table_count(self):
        try:
            response = self.client.table(self.table_name).select('*', count='exact').execute()
            if response.count is not None:
                logging.info(f"✓ Retrieved count from '{self.table_name}' via REST API: {response.count} entries")
                return response.count
            else:
                logging.warning(f"Could not retrieve count from '{self.table_name}'.")
                return None
        except Exception as e:
            logging.error(f"✗ Error counting data in '{self.table_name}': {e}")
            return None

    def delete_random_entry(self):
        try:
            # Fetch all IDs from the table
            response = self.client.table(self.table_name).select('id').execute()
            if response.data:
                ids = [item['id'] for item in response.data]
                if not ids:
                    logging.info(f"No entries to delete in '{self.table_name}'.")
                    return True  # No deletion needed, but not an error

                # Randomly select one ID to delete
                import random
                random_id = random.choice(ids)

                # Delete the entry with the selected ID
                self.client.table(self.table_name).delete().eq('id', random_id).execute()
                logging.info(f"✓ Successfully deleted entry (id: {random_id}) from '{self.table_name}' via REST API")
                return True
            else:
                logging.warning(f"No data retrieved from '{self.table_name}'.")
                return False
        except Exception as e:
            logging.error(f"✗ Error deleting data from '{self.table_name}': {e}")
            return False
