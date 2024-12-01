from flask import Flask, request, render_template_string, redirect, url_for, jsonify
import os

app = Flask(__name__)

# Route for the homepage with input form
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        link = request.form['link']
        # Process the link as needed here
        return redirect(url_for('result', link=link))
    return render_template_string(open('index.html').read())

# Route for processing the link and displaying results
@app.route('/result')
def result():
    link = request.args.get('link')
    if not link:
        return "No link provided", 400
    
    # Here, add logic to process the link as required (e.g., web scraping)
    result = f"Processing link: {link}"
    
    return render_template_string(open('result.html').read(), result=result)

# Run the app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)


