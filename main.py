import psycopg2
import time
# executed from console python main.py

def connect_db():
    """Connect to PostgreSQL database"""
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="testdb",
        user="postgres",
        password="postgres"
    )

def insert_user(cursor, name, email):
    """Insert a new user into the database"""
    cursor.execute(
        "INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id",
        (name, email)
    )
    user_id = cursor.fetchone()[0]
    print(f"✓ Inserted user: {name} ({email}) with ID {user_id}")
    return user_id

def update_user(cursor, user_id, new_email):
    """Update a user's email"""
    cursor.execute(
        "UPDATE users SET email = %s WHERE id = %s",
        (new_email, user_id)
    )
    print(f"✓ Updated user ID {user_id} with new email: {new_email}")

def delete_user(cursor, user_id):
    """Delete a user"""
    cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
    print(f"✓ Deleted user ID {user_id}")

def main():
    print("=== Debezium CDC Demo ===\n")
    print("Connecting to PostgreSQL...")

    conn = connect_db()
    cursor = conn.cursor()

    try:
        # Insert new users
        print("\n1. Inserting new users...")
        user_id1 = insert_user(cursor, "Charlie", "charlie@example.com")
        conn.commit()
        time.sleep(2)

        user_id2 = insert_user(cursor, "Diana", "diana@example.com")
        conn.commit()
        time.sleep(2)

        # Update a user
        print("\n2. Updating user...")
        update_user(cursor, user_id1, "charlie.new@example.com")
        conn.commit()
        time.sleep(2)

        # Delete a user
        print("\n3. Deleting user...")
        delete_user(cursor, user_id2)
        conn.commit()
        time.sleep(2)

        print("\n✓ All operations completed!")
        print("\nCheck Redis for CDC events:")
        print("  redis-cli XREAD COUNT 10 STREAMS dbserver1.public.users 0")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
