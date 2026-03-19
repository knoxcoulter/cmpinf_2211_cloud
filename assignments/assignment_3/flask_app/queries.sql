-- Write SQL queries

use wizard_books;

-- get all books
SELECT b.book_id, b.title, b.isbn, b.published_year, b.price, b.publisher_id,
	   CONCAT(a.first_name, ' ', a.last_name) AS author,
	   COALESCE(SUM(i.quantity), 0) AS inventory
FROM books b
LEFT JOIN book_author ba ON b.book_id = ba.book_id
LEFT JOIN authors a ON ba.author_id = a.author_id
LEFT JOIN inventory i ON b.book_id = i.book_id
GROUP BY b.book_id, 7;

-- insert a book
INSERT INTO books (title, isbn, published_year, price, publisher_id) VALUES ("Clean Code", "9780132350884", 2008, 49.99, 1);

-- return a single book, ex = book_id = 40
SELECT b.book_id, b.title, b.isbn, b.published_year, b.price, b.publisher_id,
	   CONCAT(a.first_name, ' ', a.last_name) AS author,
	   COALESCE(SUM(i.quantity), 0) AS inventory
FROM books b
LEFT JOIN book_author ba ON b.book_id = ba.book_id
LEFT JOIN authors a ON ba.author_id = a.author_id
LEFT JOIN inventory i ON b.book_id = i.book_id
WHERE b.book_id = 40
GROUP BY b.book_id, 7;

-- patch a book's inventory
UPDATE inventory SET quantity = 10 WHERE book_id = 40;

-- delete a book
DELETE FROM books WHERE book_id = 40;