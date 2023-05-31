from typing import Optional
from fastapi import FastAPI, Response, status, HTTPException
from fastapi.params import Body
from pydantic import BaseModel
from random import randrange
import psycopg2
from psycopg2.extras import RealDictCursor
import time
import pymssql
import pandas as pd

app = FastAPI()

class Post(BaseModel):
    title: str
    content: str
    published: bool = True
    rating: Optional[int] = None
class XuatFileRequest(BaseModel):
    ltc_id: int
    file_name: str

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
    colnames = [desc[0] for desc in cursor.description]
    print(colnames)
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
@app.post("/xuatfile")
def xuatfile(xuatFileRequest: XuatFileRequest):
    server = "localhost:1433"
    user = "sa"
    password = "123456"
    sql = '''select tam3.sinhvien_id, tam3.ngay, isnull(dd.trang_thai,0) trangthai into #t1 from (select * from 
    (select sinhvien_id from loptinchi_sinhvien where loptinchi_id = %d) tam1,
    (select tam5.ngay_id,tam4.ngay from ngay tam4 join (select ngay_id from loptinchingay where loptinchi_id = %d) tam5 on tam4.id = tam5.ngay_id ) tam2) tam3 left join diemdanh dd on tam3.ngay_id = dd.ngay_id and tam3.sinhvien_id = dd.sinhvien_id ORDER BY tam3.sinhvien_id


    select tam4.ngay into #tam6 from ngay tam4 join (select ngay_id from loptinchingay where loptinchi_id = %d) tam5 on tam4.id = tam5.ngay_id

    declare @colnameList varchar (MAX)
    set @colnameList = NULL
    SELECT @colnameList =  COALESCE( + @colnameList + '],[ ', '') + convert(varchar,ngay)
    FROM #tam6
    SET @colnameList = '[' + @colnameList + ']'
    declare @SQLQuery NVARCHAR(MAX)
    set @SQLQuery = 'SELECT sinhvien_id, '+ @colnameList+' into #t2 FROM #t1
    PIVOT
    (AVG(trangthai) FOR ngay IN (' + @colnameList+'))  AS PivotTable;  select u.hoten, t.* from #t2 t join users u on t.sinhvien_id = u.id'

    exec(@SQLQuery)'''
    conn1 = pymssql.connect(server='127.0.0.1', user='sa', password='123456', database='final_attendance_system')
    cursor1 = conn1.cursor(as_dict=True)
    cursor1.execute(sql, (xuatFileRequest.ltc_id,xuatFileRequest.ltc_id,xuatFileRequest.ltc_id,))
    column_names = [item[0] for item in cursor1 .description]
    print(column_names)
    # convert into dataframe
    df = pd.DataFrame(data=cursor1)

    #convert into excel
    df.to_excel(xuatFileRequest.file_name + '.xlsx', index=False)
    for row in cursor1:
        #print("Email=%s, Hoten=%s" % (row['email'], row['hoten']))
        print(row)

    conn1.close()



