import grpc
import json
import database_pb2
import database_pb2_grpc

class DatabaseClient:
    def __init__(self, master_address="localhost:50050"):
        self.channel = grpc.insecure_channel(master_address)
        self.stub = database_pb2_grpc.DatabaseServiceStub(self.channel)
        self.current_db = None

    def execute_command(self, command):
        parts = command.strip().split()
        if not parts:
            return "Empty command"

        cmd = parts[0].lower()

        if cmd == "create" and len(parts) == 2:
            return self.create_database(parts[1])
        elif cmd == "use" and len(parts) == 2:
            return self.use_database(parts[1])
        elif cmd == "list":
            return self.list_databases()
        elif cmd == "delete" and len(parts) == 2:
            return self.delete_database(parts[1])
        elif cmd == "insert" and len(parts) >= 3:
            return self.create_document(parts[1], " ".join(parts[2:]))
        elif cmd == "read" and len(parts) == 2:
            return self.read_document(parts[1])
        elif cmd == "readall":
            return self.read_all_documents()
        elif cmd == "update" and len(parts) >= 3:
            return self.update_document(parts[1], " ".join(parts[2:]))
        elif cmd == "delete_doc" and len(parts) == 2:
            return self.delete_document(parts[1])
        elif cmd == "clear":
            return self.clear_database()
        else:
            return "Invalid command. Examples: create mydb, insert mydb {'key': 'value'}, read doc_id"

    def create_database(self, db_name):
        response = self.stub.CreateDatabase(database_pb2.DatabaseName(name=db_name))
        return response.message

    def use_database(self, db_name):
        response = self.stub.UseDatabase(database_pb2.DatabaseName(name=db_name))
        if response.success:
            self.current_db = db_name
        return response.message

    def list_databases(self):
        response = self.stub.ListDatabases(database_pb2.Empty())
        return "Databases: " + ", ".join(response.names) if response.names else "No databases"

    def delete_database(self, db_name):
        response = self.stub.DeleteDatabase(database_pb2.DatabaseName(name=db_name))
        return response.message

    def create_document(self, doc_id, document_json):
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        try:
            json.loads(document_json)  # Validate JSON
            response = self.stub.CreateDocument(
                database_pb2.DocumentRequest(
                    db_name=self.current_db,
                    document=document_json,
                    doc_id=doc_id
                ),
                timeout=10  # 10 second timeout
            )
            return f"Created document with ID: {response.doc_id}"
        except grpc.RpcError as e:
            return f"Error: {e.code().name}: {e.details()}"
        except json.JSONDecodeError:
            return "Invalid JSON format"
        except Exception as e:
            return f"Unexpected error: {str(e)}"
    
    def read_document(self, doc_id):
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        response = self.stub.ReadDocument(database_pb2.DocumentID(
            db_name=self.current_db,
            doc_id=doc_id
        ))
        return response.document if response.document else "Document not found"

    def read_all_documents(self):
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        response = self.stub.ReadAllDocuments(database_pb2.DatabaseName(name=self.current_db))
        return "\n".join(response.documents) if response.documents else "No documents"

    def update_document(self, doc_id, updates_json):
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        try:
            json.loads(updates_json)  # Validate JSON
            response = self.stub.UpdateDocument(database_pb2.UpdateRequest(
                db_name=self.current_db,
                doc_id=doc_id,
                updates=updates_json,
                create_if_missing=False
            ))
            return response.message
        except json.JSONDecodeError:
            return "Invalid JSON format"
        except Exception as e:
            return str(e)

    def delete_document(self, doc_id):
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        response = self.stub.DeleteDocument(database_pb2.DocumentID(
            db_name=self.current_db,
            doc_id=doc_id
        ))
        return response.message

    def clear_database(self):
        if not self.current_db:
            return "No database selected. Use 'use <db_name>' first."
        response = self.stub.ClearDatabase(database_pb2.DatabaseName(name=self.current_db))
        return response.message

def main():
    client = DatabaseClient()
    print("Database CLI. Type 'exit' to quit.")
    print("Commands: create <db>, use <db>, list, delete <db>, insert <doc_id> <json>, read <doc_id>, readall, update <doc_id> <json>, delete <doc_id>, clear")
    
    while True:
        command = input("> ")
        if command.lower() == "exit":
            break
        result = client.execute_command(command)
        print(result)

if __name__ == "__main__":
    main()
