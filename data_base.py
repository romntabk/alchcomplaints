from sqlalchemy import (
    create_engine, MetaData, Table,
    Column, Integer, String, union_all,
    or_, and_, tuple_, over, 
    DateTime, ForeignKey, Numeric,
    PrimaryKeyConstraint, Index, 
    cast, Date, select, literal, inspect
    )
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker, aliased
from sqlalchemy_utils import database_exists, create_database
from datetime import date, timedelta, datetime
from sqlalchemy.sql.expression import func, literal_column
from draw_charts import (
    draw_chart_new_and_changed, 
    draw_chart_number_of_complaints_for_companies
    )
from config import PASSWORD, IP, DB_NAME
import json
import downloader as dload
import timer as my_timer

#TODO
#1. speed up upload to the database : Done
#2. adding rows in bd, remove for loops : Done
#3. changing rows -> comparing nulls is bad : in process
#4. fix None in update stamp in deleted rows 
#5. add abstract class for complaint and temp_table
#6. check query for drawing, especially second one - we need actual complaints
#7. add retry on requests in downloader
#8. PEP8 format : in process

Base = declarative_base()

class AbstractComplaint(object):
    complaint_id = Column(Integer, primary_key=True)
    date_received = Column(Date, primary_key=True)
    date_sent_to_company = Column(Date, nullable=True)
    state = Column(String(50), nullable=True)   
    consumer_disputed = Column(String(50), nullable=True) 
    timely = Column(String(50), nullable=True)  
    company_response = Column(String(150), nullable=True)
    submitted_via = Column(String(50), nullable=True)  
    consumer_consent_provided = Column(String(50), nullable=True)  
    tags = Column(String(50), nullable=True) 
    zip_code = Column(String(50), nullable=True)  
    company = Column(String(150), nullable=True) 
    company_public_response = Column(String(150), nullable=True)
    complaint_what_happened = Column(String(50000), nullable=True) 
    issue = Column(String(150), nullable=True) 
    sub_issue = Column(String(150), nullable=True)
    product = Column(String(150), nullable=True) 
    sub_product = Column(String(150), nullable=True) 
    

class Complaint(AbstractComplaint, Base):
    
    __tablename__ = 'complaints'

    update_stamp = Column(
                     DateTime(), primary_key=True,\
                     index=True, server_default=func.now())

    def __repr__ (self):
    	return (f'(id: {self.complaint_id}, received: {self.date_received}, '
                f'updated: {self.update_stamp})')


class temp_table(AbstractComplaint, Base):
    __tablename__ = 'temp_table'
    def __repr__ (self):
    	return f'(id: {self.complaint_id}, received: {self.date_received})'

class AlchDataBase:
    def __init__(self, TIME_INTERVAL=31, password=PASSWORD,
                 ip=IP, db_name=DB_NAME):
        url = f"postgresql+psycopg2://postgres:{password}@{ip}/{db_name}"
        self.engine = create_engine(url, echo=False)
        self.session = Session(bind=self.engine)
        if not database_exists(self.engine.url):
            create_database(self.engine.url)
        insp = inspect(self.engine)
        self.TIME_INTERVAL = TIME_INTERVAL
        self.downloader = dload.Downloader(TIME_INTERVAL)
        if insp.has_table('temp_table'):
            temp_table.__table__.drop(self.engine)
        Base.metadata.create_all(self.engine)
        self.INITIAL_DATE = '2000-01-01'
        

    @my_timer.timer('file')
    def __get_inital_json(self):
    	file_jsn_read  = open('complaints.json', 'r')
    	return json.load(file_jsn_read)


    @my_timer.timer('Загрузка всей базы данных')
    def __initial_download(self):
        # json_data = self.downloader.download_initial_data()
        json_data = self.__get_inital_json()
        complaints_to_insert = []
        for i in json_data: 
            complaints_to_insert.append(
                i | {'update_stamp' : self.INITIAL_DATE})
            if len(complaints_to_insert)%10000 == 0: 
                self.session.execute(Complaint.__table__.insert(),
                                     complaints_to_insert)
                self.session.commit()
                complaints_to_insert = []
        if complaints_to_insert:
            self.session.execute(
                Complaint.__table__.insert(),
                complaints_to_insert)
            self.session.commit()

    @my_timer.timer('filltemptable')
    def __fill_temp_table(self, json_data):
        complaints_to_insert = []
        for i in json_data: 
            complaints_to_insert.append(
                i['_source'] | {'update_stamp' : self.INITIAL_DATE})
            if len(complaints_to_insert)%10000 == 0: 
                self.session.execute(temp_table.__table__.insert(),
                                     complaints_to_insert)
                self.session.commit()
                complaints_to_insert = []
        if complaints_to_insert:
            self.session.execute(temp_table.__table__.insert(), 
                                 complaints_to_insert)
            self.session.commit()


    @my_timer.timer('Вся загрузка данных')
    def load_changes(self):
        ''' Finds new, modified and deleted complaints
            and adds information to the database
        
        Keyword arguments:
        json_data -- downloaded recordings for the last month in json format


        '''
        number_of_rows = self.session.query(Complaint.complaint_id).count()
        if number_of_rows == 0: # TODO: Переделать, это ужасно
            self.__initial_download()
            return
        json_data = self.downloader.download_monthly_data()
        self.__fill_temp_table(json_data)
        old_data = (self.session.
            query(Complaint).
            filter(
               Complaint.date_received
               > (date.today()-timedelta(self.TIME_INTERVAL))
               )
            )

        alias = aliased(Complaint, old_data.subquery())
        actual_tuple = (self.session.
            query(alias.complaint_id,func.max(alias.update_stamp)).
            group_by(alias.complaint_id)
            )

        actual_data = (self.session.
            query(alias).
            filter(
                tuple_(alias.complaint_id, alias.update_stamp).
                in_(actual_tuple)
                )
            )

        self.__find_delete_rows(actual_data) 
        self.__find_new_rows(alias)
        self.__find_change_rows(actual_data)


    @my_timer.timer('Поиск и добавление изменённых данных')
    def __find_change_rows(self, actual_data):
        alias1 = aliased(Complaint,actual_data.subquery()) 
        changed_rows = (self.session.
            query(temp_table).
            join(alias1, alias1.complaint_id == temp_table.complaint_id).
            filter(
                or_(
                  temp_table.date_received != alias1.date_received,
                  temp_table.date_sent_to_company != alias1.date_sent_to_company,
                  temp_table.state != alias1.state,
                  temp_table.timely != alias1.timely,
                  temp_table.consumer_disputed != alias1.consumer_disputed,
                  temp_table.company_response != alias1.company_response,
                  temp_table.submitted_via != alias1.submitted_via,
                  temp_table.consumer_consent_provided != alias1.consumer_consent_provided,
                  temp_table.tags != alias1.tags,
                  temp_table.zip_code != alias1.zip_code,
                  temp_table.company != alias1.company,
                  temp_table.company_public_response != alias1.company_public_response,
                  temp_table.complaint_what_happened != alias1.complaint_what_happened,
                  temp_table.issue != alias1.issue,
                  temp_table.sub_issue != alias1.sub_issue,
                  temp_table.product != alias1.product,
                  temp_table.sub_product != alias1.sub_product
                  )
                )
            )

        column_names = [i.name for i in temp_table.__table__.columns]
        ins_query = Complaint.__table__.insert().from_select(
                                                    names = column_names,
                                                    select = changed_rows)
        print(f'Number of new complaints: {changed_rows.count()}')
        self.session.execute(ins_query)
        self.session.commit()
        
    

    @my_timer.timer('Поиск и добавление новых данных')
    def __find_new_rows(self, alias):
        actual_date = date.today() - timedelta(self.TIME_INTERVAL)
        new_rows = (self.session.
            query(temp_table).
        	outerjoin(alias, alias.complaint_id == temp_table.complaint_id).
            filter(
                alias.date_received == None,
                temp_table.date_received > actual_date
                )
            )

        column_names = [i.name for i in temp_table.__table__.columns]
        ins_query = Complaint.__table__.insert().from_select(
                                                         names=column_names,
                                                         select=new_rows)

        print(f'Number of new complaints: {new_rows.count()}')
        self.session.execute(ins_query)
        self.session.commit()
        
        
    
    @my_timer.timer('Поиск и добавление удалённых данных')
    def __find_delete_rows(self, alias):
        sub_q = self.session.query(temp_table.complaint_id)
        alias = aliased(Complaint,alias.subquery())
        non_exist = (self.session.
            query(alias).
            filter(~alias.complaint_id.in_(sub_q.subquery())).
            subquery('non_exist'))
        deleted_rows = (self.session.
            query(non_exist.c.complaint_id, non_exist.c.date_received).
            filter(
                ~and_(
                    non_exist.c.sub_issue == None, non_exist.c.state == None,
                    non_exist.c.company == None, non_exist.c.timely == None,
                    non_exist.c.product == None, non_exist.c.issue == None,
                    non_exist.c.tags == None, non_exist.c.zip_code == None,
                    non_exist.c.consumer_consent_provided == None,
                    non_exist.c.company_public_response == None,
                    non_exist.c.complaint_what_happened == None,
                    non_exist.c.consumer_disputed == None,
                    non_exist.c.company_response == None,
                    non_exist.c.submitted_via == None, 
                    non_exist.c.sub_product == None,
                    )
                )
            )  

        column_names = ['complaint_id', 'date_received']        
        ins_query = Complaint.__table__.insert().from_select(
                                                         names=column_names, 
                                                         select=deleted_rows)
        print('Number of deleted complaints:',deleted_rows.count())
        self.session.execute(ins_query) 
        self.session.commit()



    @my_timer.timer('Отрисовка графика добавлений / изменений')
    def draw_chart_new_change(self): 
        '''Draws a chart for new and changed complaints for each day'''


        new_data = (self.session.query(
                     Complaint.complaint_id,
                     func.min_(Complaint.update_stamp).label('update_stamp')
                     ).group_by(Complaint.complaint_id).subquery('new_data'))

        new_data = (self.session.
            query(new_data.c.update_stamp,func.count(new_data.c.update_stamp)).
            filter(new_data.c.update_stamp != self.INITIAL_DATE).
            group_by(new_data.c.update_stamp)
            )

        s_q = (self.session.
            query(
                Complaint.update_stamp.label('update_stamp'),
                func.count(Complaint.update_stamp).label('count')).
            group_by(Complaint.update_stamp).
            having(Complaint.update_stamp != self.INITIAL_DATE).
            subquery('s_q')
            )

        changed_data = (self.session.
            query(s_q.c.update_stamp, s_q.c.count).
            filter(~tuple_(s_q.c.update_stamp, s_q.c.count).in_(new_data))
            )

        draw_chart_new_and_changed(new_data.all(), changed_data.all())
        

    @my_timer.timer('Отрисовка жалоб для двух компаний')
    def draw_chart_company(self, company1, company2): 
        ''' Draws a chart that displays the number of complaints
        left to two companies for each day
        
        Keyword arguments:
        company1 - name of the first company
        company2 - name of the second company
        '''


        company1_complaints = (self.session.
            query(Complaint.date_received, 
                  func.count(Complaint.date_received)).
            filter(and_(Complaint.company == company1, 
                        Complaint.date_sent_to_company != None)).
            group_by(Complaint.date_received)
            )
       
        company2_complaints = (self.session.
            query(Complaint.date_received, 
                  func.count(Complaint.date_received)).
            filter(and_(Complaint.company == company2, 
                        Complaint.date_sent_to_company != None)).
            group_by(Complaint.date_received)
            )

        draw_chart_number_of_complaints_for_companies(
            company1_complaints, company2_complaints, company1, company2)
        
