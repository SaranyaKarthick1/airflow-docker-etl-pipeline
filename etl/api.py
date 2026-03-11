import os
from flask import Flask, send_file, abort

app = Flask(__name__)

# Project folder where the original CSV is stored
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(os.path.dirname(BASE_DIR), "SalesData")

@app.route("/sales/csv")
def get_sales_csv():
    file_path = os.path.join(PROJECT_ROOT, "amazon.csv")

    if os.path.exists(file_path):
        # Serve the original CSV directly
        return send_file(file_path, mimetype="text/csv", as_attachment=True)
    else:
        # Return 404 if CSV is missing
        abort(404, description="CSV file not found in SalesData folder")

if __name__ == "__main__":
    app.run(debug=True)