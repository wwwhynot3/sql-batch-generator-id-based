# SQL Batch Slicer (ID-Based)


A lightweight, interactive CLI tool that splits massive SQL operations (UPDATE/DELETE) into small, safe batches based on ID ranges.

It helps avoid long table locks and timeouts when dealing with large datasets.

✨ Features
Interactive UI: Built with inquire for easy input validation.

Smart Parsing:

Automatically removes existing ORDER BY and LIMIT clauses.

Intelligently appends the ID range to existing WHERE conditions.

External Editor: Opens your default text editor (Vim, Nano, VSCode) for SQL input.

High Performance: Streams output directly to disk (id_slice.sql), keeping memory usage low.

🚀 Quick Start
Prerequisites
Rust & Cargo installed.

Installation & Run
Bash
## 1. Clone the repo
git clone https://github.com/your-username/sql-batch-slicer.git
cd sql-batch-slicer

## 2. Run
cargo run --release
📖 Usage
Start/End ID: Enter the primary key range (e.g., 1 to 100000).

Batch Size: Enter how many rows per batch (default: 50000).

Input SQL: The tool opens your system editor. Paste your original SQL.

Output: The generated SQLs are saved to id_slice.sql.

Example
Parameters:

Start: 1, End: 100, Batch: 50

Input SQL:

SQL
UPDATE users SET active = 0 WHERE created_at < '2023-01-01' ORDER BY id DESC LIMIT 1000;
Generated Output (id_slice.sql):

SQL
UPDATE users SET active = 0 WHERE id BETWEEN 1 AND 50 AND (created_at < '2023-01-01');
UPDATE users SET active = 0 WHERE id BETWEEN 51 AND 100 AND (created_at < '2023-01-01');
⚙️ How it works
Clean: Removes tailing ORDER BY or LIMIT from the input using Regex.

Locate: Finds the last WHERE clause.

Inject: Inserts id BETWEEN start AND end and wraps original conditions in parentheses.

Stream: Iterates through the ID range and writes lines to the file.

⚠️ Note
Column Name: The tool hardcodes the primary key column as id. If your table uses user_id or similar, you must modify the writeln! macro in main.rs.

📄 License
MIT License.
