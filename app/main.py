from typing import Optional
from fastapi import FastAPI, Response, status, HTTPException
from fastapi.params import Body
from pydantic import BaseModel
from random import randrange
import psycopg2
from psycopg2.extras import RealDictCursor
import time

app = FastAPI()

class Post(BaseModel):
    title: str
    content: str
    published: bool = True
    rating: Optional[int] = None

while True:
    try:
        conn = psycopg2.connect(host = "localhost", database = "fastapi", user = "postgres", password = "1",cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        print("Database connection was succesfully")
        break
    except Exception as error:
        print("Connecting to database failed")
        print("Error", error)
        time.sleep(2)
my_posts = [{"title": "abc", "content":"abc","id":1}, {"title": "bcd", "content":"bcd","id":2}]
def find_post(id):
    for p in my_posts:
        if p["id"] == id:
            return p
def find_index_post(id):
    for i,p in enumerate(my_posts):
        if p["id"] == id:
            return i
@app.get("/")
def root():
    return {"message": "Hello World"}

@app.get("/posts")
def get_posts():
    cursor.execute("""SELECT * FROM posts""")
    posts = cursor.fetchall()
    return {"posts": posts}

@app.get("/posts/latest")
def get_latest_post():
    post = my_posts[-1]
    return {"post_detail": post}

@app.get("/posts/{id}")
def get_post(id: int, response: Response):
    cursor.execute("""SELECT * FROM posts WHERE id = %s""", (str(id),))
    post = cursor.fetchone()
    if not post:
        # response.status_code = status.HTTP_404_NOT_FOUND
        # return {"message": f"post with id: {id} was not found"}
        #better use
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail=f"post with id: {id} was not found")
    return {"post_detail": post}

@app.post("/posts", status_code=status.HTTP_201_CREATED)
def create_posts(new_post: Post):
    cursor.execute("""insert into posts(title, content, published) values (%s, %s, %s) RETURNING *""", (new_post.title, new_post.content, new_post.published))
    new_post_data = cursor.fetchone()
    return {"data" : new_post_data}

@app.put("/posts/{id}")
def update_posts(id: int, post: Post):
    cursor.execute("""update posts set title = %s, content = %s, published = %s where id = %s RETURNING *""", (post.title, post.content, post.published,str(id)))
    updated_post = cursor.fetchone()
    conn.commit()
    if updated_post == None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"post with id: {id} does not exist")
    return {"data" : updated_post}

@app.delete("/posts/{id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_posts(id: int):
    cursor.execute("""DELETE FROM posts WHERE id = %s RETURNING *""", (str(id),))
    post = cursor.fetchone()
    conn.commit()
    if post == None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"post with id: {id} does not exist")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


