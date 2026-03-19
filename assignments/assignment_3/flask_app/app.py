from flask import Flask, jsonify, request
import pymysql

app = Flask(__name__)

def get_connection():
    return pymysql.connect(
        host='mdsmysql.sci.pitt.edu',
        user='kwc15',
        password='Mds_4204465@',
        database='wizard_books',
        cursorclass=pymysql.cursors.DictCursor
    )

def find_book(book_id):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM books WHERE book_id = %s", (book_id,))
            return cursor.fetchone()
    finally:
        conn.close()

# GET all books
@app.route('/books', methods=['GET'])
def list_books():
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT b.book_id, b.title, b.isbn, b.published_year, b.price, b.publisher_id,
                    CONCAT(a.first_name, ' ', a.last_name) AS author,
                    COALESCE(SUM(i.quantity), 0) AS inventory
                FROM books b
                LEFT JOIN book_author ba ON b.book_id = ba.book_id
                LEFT JOIN authors a ON ba.author_id = a.author_id
                LEFT JOIN inventory i ON b.book_id = i.book_id
                GROUP BY b.book_id, 7
            """)
            books = cursor.fetchall()
        return jsonify(books), 200
    finally:
        conn.close()

# POST a new book
@app.route('/books', methods=['POST'])
def add_book():
    data = request.get_json()
    title = data.get('title')
    isbn = data.get('isbn')
    published_year = data.get('published_year')
    price = data.get('price')
    publisher_id = data.get('publisher_id')

    if not all([title, isbn, published_year, price]):
        return jsonify({'message': 'Required fields: title, isbn, published_year, price'}), 400

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO books (title, isbn, published_year, price, publisher_id) VALUES (%s, %s, %s, %s, %s)",
                (title, isbn, published_year, price, publisher_id)
            )
            conn.commit()
            new_id = cursor.lastrowid
            cursor.execute("SELECT * FROM books WHERE book_id = %s", (new_id,))
            new_book = cursor.fetchone()
        return jsonify({'message': 'Book added successfully', 'book': new_book}), 201
    finally:
        conn.close()

# GET a specific book
@app.route('/books/<int:book_id>', methods=['GET'])
def get_book(book_id):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT b.book_id, b.title, b.isbn, b.published_year, b.price, b.publisher_id,
                    CONCAT(a.first_name, ' ', a.last_name) AS author,
                    COALESCE(SUM(i.quantity), 0) AS inventory
                FROM books b
                LEFT JOIN book_author ba ON b.book_id = ba.book_id
                LEFT JOIN authors a ON ba.author_id = a.author_id
                LEFT JOIN inventory i ON b.book_id = i.book_id
                WHERE b.book_id = %s
                GROUP BY b.book_id, 7
            """, (book_id,))
            book = cursor.fetchone()
        if book:
            return jsonify(book), 200
        return jsonify({'message': 'Book not found'}), 404
    finally:
        conn.close()

# PATCH a book's inventory
@app.route('/books/<int:book_id>', methods=['PATCH'])
def update_book(book_id):
    book = find_book(book_id)
    if not book:
        return jsonify({'message': 'Book not found'}), 404

    data = request.get_json()
    allowed_fields = ['quantity']
    updates = {k: v for k, v in data.items() if k in allowed_fields}

    if not updates:
        return jsonify({'message': 'No valid fields provided to update'}), 400

    set_clause = ', '.join(f"{field} = %s" for field in updates)
    values = list(updates.values()) + [book_id]

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"UPDATE inventory SET {set_clause} WHERE book_id = %s", values)
            conn.commit()
            cursor.execute("SELECT * FROM inventory WHERE book_id = %s", (book_id,))
            updated_inventory = cursor.fetchone()
        return jsonify({'message': 'Book updated successfully in inventory', 'inventory': updated_inventory}), 200
    finally:
        conn.close()

# DELETE a book
@app.route('/books/<int:book_id>', methods=['DELETE'])
def delete_book(book_id):
    book = find_book(book_id)
    if not book:
        return jsonify({'message': 'Book not found'}), 404

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM books WHERE book_id = %s", (book_id,))
            conn.commit()
        return jsonify({'message': 'Book deleted successfully'}), 200
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(debug=True)