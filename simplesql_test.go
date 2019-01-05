package main

import (
	"context"
	"log"
	"simplesql"

	_ "github.com/lib/pq"
)

var ssql simplesql.Sql

//simple struct
type UserBean struct {
	Username  string //first field of the simple struct will be default key if  has no skey field
	Pwd       string //field name will be the column name if there is no scol tag
	Nick_name string
	Real_name string
	Role_id   int
}

type UserBean2 struct {
	Username  string `skey:"auto" scol:"username"` //all skey field will not be update when execute update;
	Pwd       string `scol:"pwd"`
	Nick_name string `skey:"def" scol:"nick_name"` //the skey fields will be used by where  when execute update;
	Real_name string `scol:"real_name"`
	Role_id   int    `scol:"role_id"`
}

type RoleBean struct {
	Role_id   int `skey:"auto" scol:"role_id"` //auto skey will not be used when execute insert ; tag scol will be the column name
	Role_Name string
	Auths     string
	Remark    string
}

func tinit() {
	//simplesql.New("postgresql", "postgres://cxx:123456@localhost/testdb?sslmode=disable")

	var err error
	ssql, err = simplesql.New("postgres", "postgres://postgres:postgres@localhost/dbtest?sslmode=disable")
	if err != nil {
		log.Println(err)
		return
	}

	ssql.RegistTable(&UserBean{}, "wuser")
	ssql.RegistTable(&RoleBean{}, "wrole")
}

func testinsert() {
	tinit()
	ctx := context.Background()
	r1 := &RoleBean{
		Role_Name: "rn",
		Auths:     "1,2,3,4,5",
		Remark:    "rremark",
	}
	r2 := &RoleBean{
		Role_Name: "rn",
		Auths:     "1,2,3,4,5",
		Remark:    "rremark",
	}
	u := &UserBean{
		Username:  "tony",
		Pwd:       "123",
		Nick_name: "nickname",
		Real_name: "realname",
		Role_id:   1,
	}
	er := ssql.Insert(ctx, r1, r2, u)
	if er != nil {
		log.Println(er)
	}
}
func testupdate() {
	tinit()
	ctx := context.Background()
	u := &UserBean{
		Username:  "tony",
		Pwd:       "1333",
		Nick_name: "nick1name",
		Real_name: "real2name",
		Role_id:   3,
	}
	er := ssql.Update(ctx, u)
	if er != nil {
		log.Println(er)
	}
}

func testselect() {
	tinit()
	ctx := context.Background()

	r, er := ssql.SelectOne(ctx, "wuser", "tony")
	if er != nil {
		log.Println(er)
		return
	}
	if r == nil {
		log.Println("no result")
		return
	}
	log.Println(r.(*UserBean).Real_name)
}
