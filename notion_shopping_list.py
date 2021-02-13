from notion.client import NotionClient
from notion.block import TodoBlock
from datetime import datetime
import time
import random
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relation, sessionmaker

Base = declarative_base()


class Products(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True)
    product_name = Column(String(255), nullable=False)
    group = Column(String(255))
    quantity = Column(String(2))
    purchased = Column(Boolean, nullable=False)
    timestamp = Column(String(255), nullable=False)

    def __init__(self, product_name, group, quantity, purchased, timestamp):
        self.product_name = product_name
        self.group = group
        self.quantity = quantity
        self.purchased = purchased
        self.timestamp = timestamp

    def __repr__(self):
        return f"Products({self.product_name},{self.group},{self.quantity},{self.purchased})"


class Shopping_Table:

    def __init__(self, token, page_url, session):
        self.client = NotionClient(token_v2=token)
        self.page = self.client.get_collection_view(url_or_id=page_url)
        self.shopping_dict = {}
        self.all_product = {}
        self.session = session

    def get_all_table_attributes(self):
        att_dic = self.page.collection.get()['schema']
        att_list = []
        for att in att_dic:
            att_list.append((att_dic[att]['name'], att_dic[att]['type']))
        return att_list

    def add_to_shoping_dict(self):
        for row in self.page.collection.get_rows():
            if not (row.purchased) and not (row.product_name in self.shopping_dict):
                self.shopping_dict[row.product_name] = {'group': row.group, 'quantity': row.quantity,
                                                        'purchased': row.purchased, 'timestamp': datetime.now()}
                self.all_product[row.product_name] = {'group': row.group, 'quantity': row.quantity,
                                                      'purchased': row.purchased, 'timestamp': datetime.now()}
            else:
                self.all_product[row.product_name] = {'group': row.group, 'quantity': row.quantity,
                                                      'purchased': row.purchased, 'timestamp': datetime.now()}
        return

    def check_if_purchased(self):
        list_to_delete = []
        for row in self.page.collection.get_rows():
            if row.purchased and row.product_name in self.shopping_dict:
                list_to_delete.append(row.product_name)
        return list_to_delete

    def delete_from_shopping_dict(self, deletion_list):
        for product in deletion_list:
            group = self.shopping_dict[product]['group']
            quantity = self.shopping_dict[product]['quantity']
            purchased = True
            timestamp = str(self.shopping_dict[product]['timestamp'])
            product_to_add = Products(product, group, quantity, purchased, timestamp)
            try:
                self.session.add(product_to_add)
                self.session.commit()
            except:
                self.session.rollback()
                print(f'was not able to add {product} to database')

            del self.shopping_dict[product]
            self.all_product[product]['purchased'] = True
        return

    def clean_database(self):
        all_records = self.session.query(Products).all()
        try:
            for rec in all_records:
                if rec.quantity == "0":
                    try:
                        self.session.delete(rec)
                        print(f"rec deleted: {rec}")
                        self.session.commit()
                    except:
                        self.session.rollback()
        except:
            print("problem - outer loop")
            self.session.rollback()

    def run(self):
        steps = 10000
        i = 0
        while i < steps:
            self.add_to_shoping_dict()
            print(f"list_size = {len(self.shopping_dict)}")
            delete_list = self.check_if_purchased()
            self.delete_from_shopping_dict(delete_list)
            print(f"round: {i} | product that purchased: {delete_list}")
            time.sleep(10)
            i += 1

    def real_run(self):
        self.add_to_shoping_dict()
        print(f"list_size = {len(self.shopping_dict)}")
        delete_list = self.check_if_purchased()
        self.delete_from_shopping_dict(delete_list)


def Main():
    while True:
        try:
            "new session - shopping list"
            try:
                engine = create_engine('mysql+mysqlconnector://itda:28031994@127.0.0.1:3306/testdatabase')
                Base.metadata.create_all(engine)
                Session = sessionmaker(bind=engine)
                session = Session()
            except:
                print("error2")

            TOKEN_V_2 = "e82f30cd27076b422ce1adab0767972fa13a3f98ae28884948af098d7c6195d1096a006c2c5ee0e719aaf79cba6f7c8ceae15e2ffc98abde445f8d4670b666d1018c2a268c91a62ec08feeaa145d"
            url = "https://www.notion.so/0eaf48edc22e45eea0026c08ca84795b?v=299a06daa0eb488aa5b7b1bbd3a71fa8"

            shops = Shopping_Table(token=TOKEN_V_2, page_url=url, session=session)
            shops.run()
            # return shops.real_run
        except:
            pass


if __name__ == '__main__':
    Main()