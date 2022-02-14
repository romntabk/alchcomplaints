from sqlalchemy import create_engine, MetaData, Table, Integer, String, union_all, or_ ,tuple_,over,\
    Column, DateTime, ForeignKey, Numeric, PrimaryKeyConstraint, Index, and_,cast,Date,select,literal,inspect#,literal_column
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from sqlalchemy.orm import Session, sessionmaker,aliased
from sqlalchemy_utils import database_exists, create_database
import timer as my_timer
import json
from datetime import date, timedelta
from sqlalchemy.sql.expression import func, literal_column
from draw_charts import draw_chart_new_and_changed,draw_chart_number_of_complaints_for_companies
import downloader as dload
from config import PASSWORD, IP, DB_NAME

Base = declarative_base()
class Complaint(Base):
    __tablename__ = 'complaints'
    complaint_id=Column(Integer,primary_key=True)
    date_received=Column(Date,primary_key=True)
    date_sent_to_company=Column(Date,nullable=True)
    state=Column(String(50),nullable=True)   #  !
    consumer_disputed=Column(String(50),nullable=True)  #  !
    timely=Column(String(50),nullable=True)    #  !
    company_response=Column(String(150),nullable=True) #  !
    submitted_via=Column(String(50),nullable=True)  #   !
    consumer_consent_provided=Column(String(50),nullable=True)   #  !
    tags=Column(String(50),nullable=True) # !
    zip_code=Column(String(50),nullable=True)  
    company=Column(String(150),nullable=True) 
    company_public_response=Column(String(150),nullable=True) #  1
    complaint_what_happened=Column(String(50000),nullable=True) 
    issue=Column(String(150),nullable=True) 
    sub_issue=Column(String(150),nullable=True)
    product=Column(String(150),nullable=True) #  ! 
    sub_product=Column(String(150),nullable=True) #  ! 
    update_stamp=Column(DateTime(), primary_key=True, nullable=True, index=True, server_default=func.now()) # Сделать тип DateTime, вдруг в один день изменения произойдут
    __table_args__ = (
        PrimaryKeyConstraint(complaint_id,date_received,update_stamp),
    )

    def __repr__ (self):
    	return f'{self.complaint_id} {self.date_received} {self.update_stamp}'


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
    def __init__(self,TIME_INTERVAL=31,password=PASSWORD,ip=IP,db_name=DB_NAME):
        self.engine = create_engine(f"postgresql+psycopg2://postgres:{password}@{ip}/{db_name}",echo=False)
        self.session = Session(bind=self.engine)
        if not database_exists(self.engine.url):
            create_database(self.engine.url)
        insp=inspect(self.engine)
        self.TIME_INTERVAL = TIME_INTERVAL
        self.downloader = dload.Downloader(TIME_INTERVAL)
        if insp.has_table('temp_table'):
            temp_table.__table__.drop(self.engine) # drop if exist
        Base.metadata.create_all(self.engine)
        self.INITIAL_DATE = '2000-01-01'
        

    @my_timer.timer('file')
    def __get_inital_json(self): # переделать
    	file_jsn_read  = open('complaints.json','r')
    	return json.load(file_jsn_read)


    @my_timer.timer('Загрузка всей базы данных')
    def __initial_download(self):
        json_data = self.downloader.download_initial_data()
        # json_data = self.__get_inital_json()
        complaints_to_insert=[]
        for i in json_data: 
            complaints_to_insert.append(i | {'update_stamp' : self.INITIAL_DATE})
            if len(complaints_to_insert)%10000==0: 
                self.session.execute(Complaint.__table__.insert(),complaints_to_insert)
                self.session.commit()
                complaints_to_insert = []
        if complaints_to_insert:
            self.session.execute(Complaint.__table__.insert(),complaints_to_insert)
            self.session.commit()


    def __fill_temp_table(self,json_data):
        for i,row in enumerate(json_data):
            self.session.add(temp_table(**(row['_source'])))
            if i%10000==0:
                self.session.commit()
        self.session.commit()


    @my_timer.timer('Вся загрузка данных')
    def load_changes(self):
        ''' Finds new, modified and deleted complaints
            and adds information to the database
        
        Keyword arguments:
        json_data -- downloaded recordings for the last month in json format


        '''
        number_of_rows = self.session.query(Complaint.complaint_id).count()
        if number_of_rows ==0: # TODO: Переделать, это ужасно
            self.__initial_download()
            return
        json_data = self.downloader.download_monthly_data()
        self.__fill_temp_table(json_data)
        old_data =self.session.query(Complaint).\
            filter(Complaint.date_received > date.today()-timedelta(self.TIME_INTERVAL)).subquery()
        alias=aliased(Complaint,old_data)
        actual_tuple = self.session.query(alias.complaint_id,func.max(alias.update_stamp)).group_by(alias.complaint_id)
        actual_data = self.session.query(alias).filter(tuple_(alias.complaint_id,alias.update_stamp).in_(actual_tuple))
        self.__find_delete_rows(actual_data) 
        self.__find_new_rows(alias)
        self.__find_change_rows(actual_data)


    @my_timer.timer('Поиск и добавление изменённых данных')
    def __find_change_rows(self,actual_data):
        alias1 = aliased(Complaint,actual_data.subquery()) 
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
        self.__add_changed_rows(changed_rows)

        
    def __add_changed_rows(self,rows):
        d={}
        changed_rows_count=0
        for i,row in enumerate(rows):
        	changed_rows_count+=1
	        for i in row.__table__.columns:
	            d[i.name] = str(getattr(row,i.name))
	        # d['update_stamp']=date.today()
	        self.session.add(Complaint(**d))
	        if i % 10000==0:
	            self.session.commit()
        self.session.commit()
        print(f'Number of modified complaints: {changed_rows_count}')
 

    @my_timer.timer('Поиск и добавление новых данных')
    def __find_new_rows(self,alias):
        actual_date= date.today()-timedelta(self.TIME_INTERVAL)
        new_rows = self.session.query(temp_table).\
        			outerjoin(alias,alias.complaint_id==temp_table.complaint_id).\
        			filter(alias.date_received==None, temp_table.date_received>actual_date)
        print(f'Number of new complaints: {new_rows.count()}')
        self.__add_new_rows(new_rows)


    def __add_new_rows(self, rows):
        d={}
        for i,row in enumerate(rows):
	        for i in row.__table__.columns:
	            d[i.name] = str(getattr(row,i.name))
	        self.session.add(Complaint(**d))
	        if i % 10000==0:
	            self.session.commit()
        self.session.commit()
        
    
    @my_timer.timer('Поиск и добавление удалённых данных')
    def __find_delete_rows(self,alias):
        sub_q = self.session.query(temp_table.complaint_id)
        alias = aliased(Complaint,alias.subquery())
        non_exist = self.session.query(alias).\
                                filter(~alias.complaint_id.in_(sub_q.subquery()))
        self.__add_deleted_rows(non_exist)

        

    def __add_deleted_rows(self, rows):
        d={}
        del_count = 0
        for i,row in enumerate(rows):
        	flag = True
	        for i in row.__table__.columns:
	            if i.name in ['complaint_id','date_received']:
	            	d[i.name] = str(getattr(row,i.name))
	            else:
	            	if getattr(row,i.name) is not None and i.name != 'update_stamp':
	            		flag = False
	            	d[i.name] = None
	        if flag:
	        	continue
	        del_count+=1
	        self.session.add(Complaint(**d))
	        if i% 10000==0:
	            self.session.commit()
        self.session.commit()
        print(f'Number of deleted complaints: {del_count}')
	  

    @my_timer.timer('Отрисовка графика добавлений / изменений')
    def draw_chart_new_change(self): 
        '''Draws a chart for new and changed complaints for each day'''
        new_data = self.session.query(Complaint.complaint_id,func.min_(Complaint.update_stamp).label('update_stamp')).group_by(Complaint.complaint_id).subquery('new_data')
        new_data = self.session.query(new_data.c.update_stamp,func.count(new_data.c.update_stamp)).filter(new_data.c.update_stamp!=self.INITIAL_DATE).group_by(new_data.c.update_stamp)
        s_q = self.session.query(Complaint.update_stamp.label('update_stamp'),func.count(Complaint.update_stamp).label('count')).group_by(Complaint.update_stamp).having(Complaint.update_stamp!=self.INITIAL_DATE).subquery('s_q')
        changed_data = self.session.query(s_q.c.update_stamp, s_q.c.count).filter(~tuple_(s_q.c.update_stamp, s_q.c.count).in_(new_data))
        draw_chart_new_and_changed(new_data.all(),changed_data.all())
        
    @my_timer.timer('Отрисовка жалоб для двух компаний')
    def draw_chart_company(self, company1, company2): 
        ''' Draws a chart that displays the number of complaints
        left to two companies for each day
        
        Keyword arguments:
        company1 - name of the first company
        company2 - name of the second company
        '''
        company1_complaints = self.session.query(Complaint.date_received,func.count(Complaint.date_received)).filter(and_(Complaint.company==company1,Complaint.date_sent_to_company!=None)).group_by(Complaint.date_received)
        company2_complaints = self.session.query(Complaint.date_received,func.count(Complaint.date_received)).filter(and_(Complaint.company==company2,Complaint.date_sent_to_company!=None)).group_by(Complaint.date_received)
        # print(company1_complaints.all())
        draw_chart_number_of_complaints_for_companies(company1_complaints,company2_complaints,company1,company2)
        
#       