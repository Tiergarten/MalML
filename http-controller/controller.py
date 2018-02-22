from flask import Flask
app = Flask(__name__)

@app.route("/get_extract/exe/uuid/extract")
def get_extract():
	return app.send_static_file('pinatrace.out.gz')

if __name__ == '__main__':
	app.run()
