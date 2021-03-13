from flask import Flask, request

app = Flask(__name__)

dt_df = dt.fread(f_name)
df = dt_df.to_pandas()

@app.route('/')
def index():
    return 'hi'



if __name__ == '__main__':
    app.run(port=8889)
