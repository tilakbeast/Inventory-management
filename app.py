from flask import Flask, render_template

app = Flask(__name__)

# Define route for health check UI
@app.route('/health-check')
def health_check():
    return render_template('health_check.html')

# Define route for item creation UI
@app.route('/create-item')
def create_item():
    return render_template('create_item.html')

# Define route for stock management UI
@app.route('/update-stock')
def update_stock():
    return render_template('update_stock.html')

# Define route for order processing UI
@app.route('/place-order')
def place_order():
    return render_template('place_order.html')

if __name__ == '__main__':
    app.run(debug=True)
