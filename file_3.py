from sqlalchemy import create_engine, MetaData, Table, Integer, String, union_all, or_ ,tuple_,over,\
    Column, DateTime, ForeignKey, Numeric, PrimaryKeyConstraint, Index, and_,cast,Date,select,literal
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from sqlalchemy.orm import Session, sessionmaker,aliased
from sqlalchemy_utils import database_exists, create_database
import file_5 as my_timer
import json
from datetime import date, timedelta
import time
from sqlalchemy.sql.expression import func
import psycopg2
from drawer import my_f as draw_statistic


Base = declarative_base()
class Complaint(Base):
    __tablename__ = 'complaints'
    complaint_id=Column(Integer,primary_key=True)
    date_received=Column(Date,primary_key=True)
    date_sent_to_company=Column(Date,nullable=True)
    state=Column(String(50),nullable=True)
    consumer_disputed=Column(String(50),nullable=True)
    timely=Column(String(50),nullable=True)
    company_response=Column(String(150),nullable=True)
    submitted_via=Column(String(50),nullable=True)
    consumer_consent_provided=Column(String(50),nullable=True)
    tags=Column(String(50),nullable=True)
    zip_code=Column(String(50),nullable=True)
    company=Column(String(150),nullable=True)
    company_public_response=Column(String(150),nullable=True)
    complaint_what_happened=Column(String(50000),nullable=True)
    issue=Column(String(150),nullable=True)
    sub_issue=Column(String(150),nullable=True)
    product=Column(String(150),nullable=True)
    sub_product=Column(String(150),nullable=True)
    update_stamp=Column(Date(), primary_key=True,nullable=True,index=True)
    __table_args__ = (
        PrimaryKeyConstraint(complaint_id,date_received,update_stamp),
    )

    def __repr__ (self):
    	return f'{self.complaint_id} {self.date_received} {self.update_stamp}'

# Base= declarative_base()
class temp_table(Base):
    __tablename__ = 'temp_table'
    complaint_id=Column(Integer,primary_key=True)
    date_received=Column(Date)
    date_sent_to_company=Column(Date,nullable=True)
    state=Column(String(50),nullable=True)
    consumer_disputed=Column(String(50),nullable=True)
    timely=Column(String(50),nullable=True)
    company_response=Column(String(150),nullable=True)
    submitted_via=Column(String(50),nullable=True)
    consumer_consent_provided=Column(String(50),nullable=True)
    tags=Column(String(50),nullable=True)
    zip_code=Column(String(50),nullable=True)
    company=Column(String(150),nullable=True)
    company_public_response=Column(String(150),nullable=True)
    complaint_what_happened=Column(String(50000),nullable=True)
    issue=Column(String(150),nullable=True)
    sub_issue=Column(String(150),nullable=True)
    product=Column(String(150),nullable=True)
    sub_product=Column(String(150),nullable=True)
    __table_args__ = (
        PrimaryKeyConstraint(complaint_id),
    )
    def __repr__ (self):
    	return f'{self.complaint_id} {self.date_received}'

class AlchDataBase:
    def __init__(self,password='postpass',ip='localhost',db_name='dbdb'):
        self.engine = create_engine(f"postgresql+psycopg2://postgres:{password}@{ip}/{db_name}",echo=False)
        self.session = Session(bind=self.engine)
        if not database_exists(self.engine.url):
            create_database(self.engine.url)
        temp_table.__table__.drop(self.engine)
        Base.metadata.create_all(self.engine)
        self.TIME_INTERVAL = 31

    def select_last_month(self):
    	a =self.session.query(Complaint).\
    		filter(Complaint.date_received > date.today()-timedelta(self.TIME_INTERVAL))
    	# print(type(a))
    	return a

    def get_inital_json(self): # переделать
    	file_jsn_read  = open('complaints.json','r')
    	return json.load(file_jsn_read)

    def initial_download(self):
        arr=[]
        json_data = self.get_inital_json()
        c=0
        for i in json_data:
            z = i # переделать, добавить тригер 
            z['update_stamp']='2000-01-01' # подумать над этим  
            self.session.add(Complaint(**dict(z)))
            if c%10000==0: 
                self.session.commit()
        print('ok')#del
        self.session.commit()
        print('second ok')#del

    # def create_temp_table(self, json_data):
    # 	# help(select)
    # 	stmts = [select([
    # 		cast(literal(row['_source']['complaint_id']),Integer).label('id'),
    # 		cast(literal(row['_source']['date_received'][:10]),Date).label('date_received'),
    # 		cast(literal(row['_source']['date_sent_to_company'][:10]) ,Date).label('date_sent_to_company'),
		  #   cast(literal(row['_source']['state']) ,String(50)).label('state'),
		  #   cast(literal(row['_source']['consumer_disputed']) ,String(50)).label('consumer_disputed'),
		  #   cast(literal(row['_source']['timely']) ,String(50)).label('timely'),
		  #   cast(literal(row['_source']['company_response']) ,String(150)).label('company_response'),
		  #   cast(literal(row['_source']['submitted_via']) ,String(50)).label('submitted_via'),
		  #   cast(literal(row['_source']['consumer_consent_provided']) ,String(50)).label('consumer_consent_provided'),
		  #   cast(literal(row['_source']['tags']) ,String(50)).label('tags'),
		  #   cast(literal(row['_source']['zip_code']) ,String(50)).label('zip_code'),
		  #   cast(literal(row['_source']['company']) ,String(150)).label('company'),
		  #   cast(literal(row['_source']['company_public_response']) ,String(150)).label('company_public_response'),
		  #   cast(literal(row['_source']['complaint_what_happened']) ,String(50000)).label('complaint_what_happened'),
		  #   cast(literal(row['_source']['issue']) ,String(150)).label('issue'),
		  #   cast(literal(row['_source']['sub_issue']) ,String(150)).label('sub_issue'),
		  #   cast(literal(row['_source']['product']) ,String(150)).label('product'),
		  #   cast(literal(row['_source']['sub_product']) ,String(150)).label('sub_product')]) if ind ==0 else
    # 		select([literal(row['_source']['complaint_id']),literal(row['_source']['date_received'][:10]),literal(row['_source']['date_sent_to_company'][:10]),\
    # 			literal(row['_source']['state']),literal(row['_source']['consumer_disputed']),literal(row['_source']['timely']),literal(row['_source']['company_response']),\
    # 			literal(row['_source']['submitted_via']),literal(row['_source']['consumer_consent_provided']),literal(row['_source']['tags']),\
    # 			literal(row['_source']['zip_code']),literal(row['_source']['company']),literal(row['_source']['company_public_response']),\
    # 			literal(row['_source']['complaint_what_happened']),literal(row['_source']['issue']),literal(row['_source']['sub_issue']),\
    # 			literal(row['_source']['product']),literal(row['_source']['sub_product'])])
		  #    for ind,row in enumerate(json_data)]
    # 	# stmts = select([cast(literal(123212),Integer).label('id'),cast(literal(111),Integer).label('kek')]) 
    # 	subquery = union_all(*stmts)
    # 	subquery = subquery.cte(name='temp1_table')
    # 	return subquery


    def update_bd(self, new_row):
    	old_data = self.select_last_month()
    	res = old_data.filter(and_(new_row['complaint_id']==Complaint.complaint_id,\
    		Complaint.date_received==cast('2022-01-27',Date) ) )
    	
    	print(res.all(),new_row)
    	# if res.count()>0:
    	# 	print(res.count())


    @my_timer.timer('parse')
    def add_and_parse_json(self,json_data):
        count = self.session.query(Complaint.complaint_id).count()
        if count ==0: # TODO: Переделать, это ужасно
            self.initial_download()
            return
        # print(json_data[0])
        for i,row in enumerate(json_data):
        	self.session.add(temp_table(**(row['_source'])))
        	if i%10000==0:
        		self.session.commit()
        self.session.commit()
        # self.session.add(temp_table(**(json_data[0]['_source'])))
        # self.session.commit()
        # for i in json_data:
        #     assert isinstance(i,dict),'got not json'
        #     assert '_source' in i, ' _source dosn"t exist in object from api'
        #     new_data = i['_source']
        #     assert isinstance(new_data,dict), 'not json in _source'
        #     assert len(new_data.keys()) == 18, 'data-format changed'
        #     new_data['date_received']=new_data['date_received'][:10] # TODO: очень плохо
        #     # print(new_data)
        #     # self.update_bd(new_data)
        # self.update_bd(json_data)
        
        # return
        old_data = self.select_last_month()
        alias=aliased(Complaint,old_data.subquery())
        acutal_date= date.today()-timedelta(self.TIME_INTERVAL)
        print('Сколько записей было',old_data.count())
        print('Сколько скачали',self.session.query(temp_table).filter(temp_table.date_received>acutal_date).count())
        # new_rows = self.session.query(temp_table).\
        # 			outerjoin(alias,alias.complaint_id==temp_table.complaint_id).\
        # 			filter(alias.date_received==None, temp_table.date_received>acutal_date)
        # # new_rows = ol.join(temp_table,Complaint.complaint_id==temp_table.complaint_id)#.filter(Complaint.complaint_id==None)
        # changed_rows = self.session.query(temp_table).join(Complaint,Complaint.complaint_id==temp_table.complaint_id).\
        # 				filter(temp_table.date_received>acutal_date).\
        # 				filter(
        # 					or_(temp_table.date_received!=Complaint.date_received,\
        # 						temp_table.date_sent_to_company!=Complaint.date_sent_to_company,\
        # 						temp_table.state != Complaint.state,\
        # 						temp_table.timely != Complaint.timely,\
        # 						temp_table.consumer_disputed != Complaint.consumer_disputed,\
        # 						temp_table.company_response != Complaint.company_response,\
        # 						temp_table.submitted_via != Complaint.submitted_via,\
        # 						temp_table.consumer_consent_provided != Complaint.consumer_consent_provided,\
        # 						temp_table.tags != Complaint.tags,\
        # 						temp_table.zip_code != Complaint.zip_code,\
        # 						temp_table.company !=Complaint.company,\
        # 						temp_table.company_public_response != Complaint.company_public_response,\
        # 						temp_table.complaint_what_happened != Complaint.complaint_what_happened,\
        # 						temp_table.issue != Complaint.issue,\
        # 						temp_table.sub_issue != Complaint.sub_issue,\
        # 						temp_table.product !=Complaint.product,\
        # 						temp_table.sub_product !=Complaint.sub_product))
        # print('Новых записей',new_rows.count())
        # print('changed',changed_rows.count())

        # self.connection = psycopg2.connect(database="dbdb", user="postgres", password="postpass", host="localhost", port=5432)
        # self.cursor = self.connection.cursor()
        actual_tuple = self.session.query(alias.complaint_id,func.max(alias.update_stamp)).group_by(alias.complaint_id)
        actual_data = self.session.query(alias).filter(tuple_(alias.complaint_id,alias.update_stamp).in_(actual_tuple))
        self.find_delete_rows(actual_data)
        self.find_new_rows(alias)
        self.find_change_rows(actual_data)

    @my_timer.timer('so')
    def find_change_rows(self,actual_data):

        #zapros = "SELECT * FROM (SELECT * FROM Complaints C WHERE date_received > '2022-01-19' AND (complaint_id, update_stamp) IN (SELECT * FROM (SELECT complaint_id, MAX(update_stamp) FROM Complaints WHERE date_received > '2022-01-19' GROUP BY complaint_id) B )) C1 JOIN temp_table T ON T.complaint_id = C1.complaint_id WHERE (T.state, T.timely,T.consumer_disputed,T.company_response, T.submitted_via, T.consumer_consent_provided, T.tags, T.zip_code, T.company, T.company_public_response,T.complaint_what_happened,T.issue,T.sub_issue,T.product,T.sub_product) != (C1.state, C1.timely,C1.consumer_disputed,C1.company_response, C1.submitted_via, C1.consumer_consent_provided, C1.tags, C1.zip_code, C1.company, C1.company_public_response,C1.complaint_what_happened,C1.issue,C1.sub_issue,C1.product,C1.sub_product);"
        # self.cursor.execute(zapros)
        # a = self.engine.execute()
        # for v in cursor.fetchall():
        #     print(v)
        # print(date.today()-timedelta(18))
        # actual_date= date.today()-timedelta(self.TIME_INTERVAL)

        # print(actual_data.count())
        # print(actual_data.limit(5).all())
        alias1 = aliased(Complaint,actual_data.subquery()) 
        # print(actual_data.count())
        # print(self.session.query(temp_table).count())
        changed_rows = self.session.query(temp_table).join(alias1, alias1.complaint_id==temp_table.complaint_id).\
                        filter(
                          or_(temp_table.date_received!=alias1.date_received,\
                              temp_table.date_sent_to_company!=alias1.date_sent_to_company,\
                              temp_table.state != alias1.state,\
                              temp_table.timely != alias1.timely,\
                              temp_table.consumer_disputed != alias1.consumer_disputed,\
                              temp_table.company_response != alias1.company_response,\
                              temp_table.submitted_via != alias1.submitted_via,\
                              temp_table.consumer_consent_provided != alias1.consumer_consent_provided,\
                              temp_table.tags != alias1.tags,\
                              temp_table.zip_code != alias1.zip_code,\
                              temp_table.company !=alias1.company,\
                              temp_table.company_public_response != alias1.company_public_response,\
                              temp_table.complaint_what_happened !=alias1.complaint_what_happened,\
                              temp_table.issue != alias1.issue,\
                              temp_table.sub_issue != alias1.sub_issue,\
                              temp_table.product !=alias1.product,\
                              temp_table.sub_product !=alias1.sub_product))
        # print('sosubibu') # 1247  |  48  47.75  47.42

        self.add_changed_rows(changed_rows)
        # print(a.filter(temp_table.state is None).count())
        return
        # changed_rows = self.session.query(alias).join(temp_table,alias.complaint_id==temp_table.complaint_id).\
        # 				filter(temp_table.date_received>actual_date).\
        # 				filter(
        # 					or_(temp_table.date_received!=alias.date_received,\
        # 						temp_table.date_sent_to_company!=alias.date_sent_to_company,\
        # 						temp_table.state != alias.state,\
        # 						temp_table.timely != alias.timely,\
        # 						temp_table.consumer_disputed != alias.consumer_disputed,\
        # 						temp_table.company_response != alias.company_response,\
        # 						temp_table.submitted_via != alias.submitted_via,\
        # 						temp_table.consumer_consent_provided != alias.consumer_consent_provided,\
        # 						temp_table.tags != alias.tags,\
        # 						temp_table.zip_code != alias.zip_code,\
        # 						temp_table.company !=alias.company,\
        # 						temp_table.company_public_response != alias.company_public_response,\
        # 						temp_table.complaint_what_happened !=alias.complaint_what_happened,\
        # 						temp_table.issue != alias.issue,\
        # 						temp_table.sub_issue != alias.sub_issue,\
        # 						temp_table.product !=alias.product,\
        # 						temp_table.sub_product !=alias.sub_product))	
       	# common_rows = self.session.query(alias).join(temp_table,alias.complaint_id==temp_table.complaint_id)
       	# alias_common = aliased(Complaint,common_rows.subquery())
       	# actual = self.session.query(alias_common.complaint_id,func.max(alias_common.update_stamp)).group_by(alias_common.complaint_id)
       	#SELECT COUNT(*) FROM (SELECT * FROM Complaints C WHERE date_received > '2022-01-27' AND (complaint_id, update_stamp) IN (SELECT * FROM (SELECT complaint_id, MAX(update_stamp) FROM Complaints WHERE date_received > '2022-01-27' GROUP BY complaint_id))) C1 JOIN temp_table T ON T.complaint_id = C1.complaint_id WHERE (T.state, T.timely,T.consumer_disputed,T.company_response, T.submitted_via, T.consumer_consent_provided, T.tags, T.zip_code, T.company, T.company_public_response,T.complaint_what_happened,T.issue,T.sub_issue,T.product,T.sub_product) != (C1.state, C1.timely,C1.consumer_disputed,C1.company_response, C1.submitted_via, C1.consumer_consent_provided, C1.tags, C1.zip_code, C1.company, C1.company_public_response,C1.complaint_what_happened,C1.issue,C1.sub_issue,C1.product,C1.sub_product)
       	####
       	
       	# print(actual.limit(5).all())
       	# print(actual.count())
        # alias1=aliased(Complaint,changed_rows.subquery())
        # actual_data_for_change = self.session.query(alias1.complaint_id,func.max(alias1.update_stamp)).group_by(alias1.complaint_id)#.subquery()
        self.add_changed_rows(changed_rows,actual)
    def add_changed_rows(self,rows):
        d={}
        changed_rows_count=0
        for i,row in enumerate(rows):
        	changed_rows_count+=1
	        for i in row.__table__.columns:
	            d[i.name] = str(getattr(row,i.name))
	        d['update_stamp']=date.today()
	        self.session.add(Complaint(**d))
	        if i % 10000==0:
	            self.session.commit()
        self.session.commit()
        print('changed_rows:',changed_rows_count)

    def find_new_rows(self,alias):
        actual_date= date.today()-timedelta(self.TIME_INTERVAL)
        new_rows = self.session.query(temp_table).\
        			outerjoin(alias,alias.complaint_id==temp_table.complaint_id).\
        			filter(alias.date_received==None, temp_table.date_received>actual_date)
        print('new_rows:',new_rows.count())
        self.add_new_rows(new_rows)


    def add_new_rows(self, rows):
        d={}
        for i,row in enumerate(rows):
	        for i in row.__table__.columns:
	            d[i.name] = str(getattr(row,i.name))
	        d['update_stamp']=date.today()
	        self.session.add(Complaint(**d))
	        if i % 10000==0:
	            self.session.commit()
        self.session.commit()
        

#   A     B




    def find_delete_rows(self,alias):
        #SELECT * FROM alias WHERE id NOT IN (SELECT id FROM temp_table)
        sub_q = self.session.query(temp_table.complaint_id)
        # actual_date= date.today()-timedelta(self.TIME_INTERVAL)
        alias = aliased(Complaint,alias.subquery())
        non_exist = self.session.query(alias).\
        filter(~alias.complaint_id.in_(sub_q.subquery()))
        # print(non_exist.count())
        # deleted_rows=alias.filter(temp_table, temp_table.complaint_id ==  alias.complaint_id).filter(temp_table.date_received== None)# удаленные комментарии.
        # alias=aliased(Complaint,deleted_rows.subquery())
        # actual_data_for_delete = self.session.query(alias.complaint_id,func.max(alias.update_stamp)).group_by(alias.complaint_id)#.subquery()
        self.add_deleted_rows(non_exist)
        

    def add_deleted_rows(self, rows):
        d={}
        del_count = 0
        for i,row in enumerate(rows):
        	# if (row.complaint_id, row.update_stamp) not in actual_data:
        	# 	continue
        	flag = True
	        for i in row.__table__.columns:
	            if i.name in ['complaint_id','date_received']:
	            	d[i.name] = str(getattr(row,i.name))
	            else:
	            	if getattr(row,i.name) is not None and i.name != 'update_stamp':
	            		flag = False
	            	d[i.name] = None
	        d['update_stamp']=date.today()
	        if flag:
	        	continue
	        del_count+=1
	        self.session.add(Complaint(**d))
	        # print(d)
	        if i% 10000==0:
	            self.session.commit()
        self.session.commit()
        print(f'Найдено удаленных записей: {del_count}')
	    # Complaint(**d)
        pass
    # def add_new_rows(self, rows):
    #     d={}
    #     for i in new_rows[0].__table__.columns:
    #         d[i.name] = str(getattr(new_rows[0],i.name))
    #     d['update_stamp']=date.today()
    #     Complaint(**d)
    #     pass

    @my_timer.timer('stat')
    def get_statistic_update_stamp(self): # Работает за 7 секунд, сомнительно
        print('1')
        # new_data = self.session.query(Complaint.complaint_id,func.min_(Complaint.update_stamp).label('update_stamp')).group_by(Complaint.complaint_id)#.cte(name='new_data')
        new_data = self.session.query(Complaint.complaint_id,func.min_(Complaint.update_stamp).label('update_stamp')).group_by(Complaint.complaint_id).subquery('new_data')
        new_data = self.session.query(new_data.c.update_stamp,func.count(new_data.c.update_stamp)).filter(new_data.c.update_stamp!='2000-01-01').group_by(new_data.c.update_stamp)
        # print(new_data2.all())
        # q = self.session.query(Complaint.update_stamp,func.count(Complaint.update_stamp)).filter(or_(new_data.c.complaint_id!=Complaint.complaint_id,new_data.c.update_stamp!=Complaint.update_stamp)).group_by(Complaint.update_stamp)
        # q = self.session.query(Complaint.update_stamp,func.count(Complaint.update_stamp)).select_from(Complaint).outerjoin(new_data, new_data.c.complaint_id==Complaint.complaint_id,new_data.c.update_stamp==Complaint.update_stamp).filter(Complaint.state == None).group_by(Complaint.update_stamp)
        al = aliased(Complaint)
        changed_data = self.session.query(Complaint.update_stamp,func.count(Complaint.update_stamp)).filter(Complaint.update_stamp!=(self.session.query(func.min_(al.update_stamp)).filter(al.complaint_id==Complaint.complaint_id).scalar())).group_by(Complaint.update_stamp)
        # print(q.all())
        # changed_data = self.session.query(Complaint.complaint_id, )
        # print(new_data.column_descriptions)
        # help(new_data)
        # alias=aliased(new_data)
        # a = self.session.query(Complaint.update_stamp,,func.count(Complaint.update_stamp))
        # a= self.session.query(my_cte.complaint_id)
        # print(new_data)
        # res = self.session.query(Complaint.update_stamp,func.count(Complaint.update_stamp)).filter(tuple_(Complaint.complaint_id,Complaint.update_stamp).in_(new_data)).group_by(Complaint.update_stamp)
        
        # res2 = self.session.query(Complaint.complaint_id,over(func.row_number(),partition_by=Complaint.update_stamp)).group_by(Complaint.complaint_id)
        # initial_data_count = self.session.query(Complaint.complaint_id,Complaint.update_stamp).filter(Complaint.update_stamp=='2000-01-01').count()
        # other_data = self.session.query(Complaint.complaint_id, func.min_(Complaint.update_stamp)).filter(Complaint.update_stamp!='2000-01-01').group_by(Complaint.complaint_id)
        # changed_data = self.session.query((Complaint.update_stamp),func.count(Complaint.update_stamp)).filter(~tuple_(Complaint.complaint_id,Complaint.update_stamp).in_(new_data)).group_by(Complaint.update_stamp)
        # print(changed_data.count())
        # новые записи - min(update_stamp) group_by id
        # count(*) group by update_stamp
        # изменённые (удаления + изменения) все кроме min(update_stamp) 
        # пока хз
        # print(res.all())
        # print(new_data.count())
        # print(initial_data_count+other_data.count()) # 3.3  1.4
        print(new_data.all(),changed_data.all())
        draw_statistic(new_data.all(),changed_data.all())

    @my_timer.timer('fun')
    def do_fun(self):
        a = self.session.query(Complaint).filter(Complaint.date_received).all()
        print('##',a,'##')

#id date   update
# 1 a b c -
# 2 d e f -
# 3 g k l -
# 4 x y z A
# 5 n m k A
# 4 x - - B
# 5 n F F C
# 
# 
# id_complaint date_received
#
#