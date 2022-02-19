import downloader as dload
from timer import timer
from config import PASSWORD, IP, DB_NAME

import json
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker, aliased
from sqlalchemy_utils import database_exists, create_database
from datetime import date, timedelta, datetime
from sqlalchemy.sql.expression import func, literal_column
from sqlalchemy import (
    create_engine, MetaData, Table,
    Column, Integer, String, union_all,
    or_, and_, tuple_, over, 
    DateTime, ForeignKey, Numeric,
    PrimaryKeyConstraint, Index, 
    cast, Date, select, literal, inspect
    )
from draw_charts import (
    draw_chart_new_and_changed, 
    draw_chart_number_of_complaints_for_companies
    )


Base = declarative_base()

class AbstractComplaint:

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
        DateTime(), 
        primary_key=True,
        index=True, 
        server_default=func.now()
        )


    @staticmethod
    def is_not_deleted_row(table):
        return ~and_(
            table.c.sub_issue == None, table.c.state == None,
            table.c.company == None, table.c.timely == None,
            table.c.product == None, table.c.issue == None,
            table.c.tags == None, table.c.zip_code == None,
            table.c.consumer_consent_provided == None,
            table.c.company_public_response == None,
            table.c.complaint_what_happened == None,
            table.c.consumer_disputed == None,
            table.c.company_response == None,
            table.c.submitted_via == None, 
            table.c.sub_product == None,
            )


    @staticmethod
    def is_not_equals_rows(row1,row2):
        return or_(
          func.coalesce(row1.state, '') != func.coalesce(row2.state, ''),
          func.coalesce(row1.timely, '') != func.coalesce(row2.timely, ''),
          func.coalesce(row1.consumer_disputed, '') != func.coalesce(row2.consumer_disputed, ''),
          func.coalesce(row1.company_response, '') != func.coalesce(row2.company_response, ''),
          func.coalesce(row1.submitted_via, '') != func.coalesce(row2.submitted_via, ''),
          func.coalesce(row1.consumer_consent_provided, '') != func.coalesce(row2.consumer_consent_provided, ''),
          func.coalesce(row1.tags, '') != func.coalesce(row2.tags, ''),
          func.coalesce(row1.zip_code, '') != func.coalesce(row2.zip_code, ''),
          func.coalesce(row1.company, '') != func.coalesce(row2.company, ''),
          func.coalesce(row1.company_public_response, '') != func.coalesce(row2.company_public_response, ''),
          func.coalesce(row1.complaint_what_happened, '') != func.coalesce(row2.complaint_what_happened, ''),
          func.coalesce(row1.issue, '') != func.coalesce(row2.issue, ''),
          func.coalesce(row1.sub_issue, '') != func.coalesce(row2.sub_issue, ''),
          func.coalesce(row1.product, '') != func.coalesce(row2.product, ''),
          func.coalesce(row1.sub_product, '') != func.coalesce(row2.sub_product, '')
          )

    def __repr__ (self):
    	return (f'(id: {self.complaint_id}, ' 
                f'received: {self.date_received}, '
                f'updated: {self.update_stamp})')


class temp_table(AbstractComplaint, Base):

    __tablename__ = 'temp_table'


    def __repr__ (self):
    	return f'(id: {self.complaint_id}, received: {self.date_received})'


class AlchDataBase:

    INITIAL_DATE = '2000-01-01'


    def __init__(self, time_interval=31, password=PASSWORD,
                 ip=IP, db_name=DB_NAME):
        url = f"postgresql+psycopg2://postgres:{password}@{ip}/{db_name}"
        self.engine = create_engine(url, echo=False)
        self.session = Session(bind=self.engine)
        if not database_exists(self.engine.url):
            create_database(self.engine.url)
        insp = inspect(self.engine)
        self.time_interval = time_interval
        self.downloader = dload.Downloader(time_interval)
        if insp.has_table('temp_table'):
            temp_table.__table__.drop(self.engine)
        Base.metadata.create_all(self.engine)


    @timer('All data loading and searching')
    def load_changes(self):
        ''' Finds new, modified and deleted complaints
        and adds information to the database
        
        Keyword arguments:
        json_data -- downloaded recordings for the last month in json format
        '''

        number_of_rows = (self.session
            .query(Complaint.complaint_id)
            .count()
            )
        if number_of_rows == 0:
            self.__initial_download_and_filling()
            return None
        json_data = self.downloader.download_monthly_data()
        self.__fill_temp_table(json_data)
        old_data = (self.session
            .query(Complaint)
            .filter(
               Complaint.date_received >
               (date.today() - timedelta(self.time_interval))
               )
            )
        alias = aliased(Complaint, old_data.subquery())
        actual_tuple = (self.session
            .query(
                alias.complaint_id, 
                func.max(alias.update_stamp)
                )
            .group_by(alias.complaint_id)
            )
        actual_data = (self.session
            .query(alias)
            .filter(
                tuple_(
                    alias.complaint_id, 
                    alias.update_stamp
                    )
                .in_(actual_tuple)
                )
            )
        return {
            'deleted' : self.__find_delete_rows(actual_data),
            'new' : self.__find_new_rows(alias),
            'changed' : self.__find_change_rows(actual_data)
            }


    @timer('Drawing new and changed data')
    def draw_chart_new_change(self): 
        '''Draws a chart for new and changed complaints for each day'''

        new_data = (self.session
            .query(
                Complaint.complaint_id,
                func.min_(Complaint.update_stamp).label('update_stamp')
                )
            .group_by(Complaint.complaint_id)
            .subquery('new_data')
            )
        new_data = (self.session
            .query(
                new_data.c.update_stamp,
                func.count(new_data.c.update_stamp)
                )
            .filter(new_data.c.update_stamp != AlchDataBase.INITIAL_DATE)
            .group_by(new_data.c.update_stamp)
            )
        s_q = (self.session
            .query(
                Complaint.update_stamp.label('update_stamp'),
                func.count(Complaint.update_stamp).label('count'))
            .group_by(Complaint.update_stamp)
            .having(Complaint.update_stamp != AlchDataBase.INITIAL_DATE)
            .subquery('s_q')
            )
        changed_data = (self.session
            .query(
                s_q.c.update_stamp, 
                s_q.c.count
                )
            .filter(
                ~tuple_(
                    s_q.c.update_stamp, 
                    s_q.c.count
                    )
                .in_(new_data))
            )
        draw_chart_new_and_changed(new_data.all(), changed_data.all())


    @timer('Drawing complaints for two companies')
    def draw_chart_company(self, company1, company2): 
        ''' Draws a chart that displays the number of complaints
        left to two companies for each day
        
        Keyword arguments:
        company1 - name of the first company
        company2 - name of the second company
        '''
        
        company1_complaints = self.__get_actual_complaints_for_company(company1)
        company2_complaints = self.__get_actual_complaints_for_company(company2)
        draw_chart_number_of_complaints_for_companies(
            company1_complaints, 
            company2_complaints, 
            company1, 
            company2
            )
 

    @timer('Filling in the temporary table')
    def __fill_temp_table(self, json_data):
        assert json_data, 'We have not received any records for the last month'  
        complaints_to_insert = []
        for json_obj in json_data: 
            assert isinstance(json_obj, dict), 'Invalid format in monthly data'
            assert '_source' in json_obj, 'Invalid format in monthly data'
            complaints_to_insert.append(
                json_obj['_source'] | {'update_stamp' : AlchDataBase.INITIAL_DATE}
                )
            if len(complaints_to_insert) % 10000 == 0: 
                self.session.execute(
                    temp_table.__table__.insert(),
                    complaints_to_insert
                    )
                self.session.commit()
                complaints_to_insert = []
        if complaints_to_insert:
            self.session.execute(
                temp_table.__table__.insert(), 
                complaints_to_insert
                )
            self.session.commit()


    @timer('Search and add changed data')
    def __find_change_rows(self, actual_data):
        alias1 = aliased(Complaint, actual_data.subquery()) 
        changed_rows = (self.session
            .query(temp_table)
            .join(
                alias1, 
                alias1.complaint_id == temp_table.complaint_id
                )
            .filter(
                  Complaint.is_not_equals_rows(temp_table,alias1)
                )
            )
        column_names = [column.name for column in temp_table.__table__.columns]
        ins_query = (Complaint.__table__.insert()
            .from_select(
                names = column_names,
                select = changed_rows
                )
            )
        number_of_changed_complaints = changed_rows.count()
        self.session.execute(ins_query)
        self.session.commit()
        return number_of_changed_complaints
        
    
    @timer('Search and add new data')
    def __find_new_rows(self, alias):
        actual_date = date.today() - timedelta(self.time_interval)
        new_rows = (self.session
            .query(temp_table)
        	.outerjoin(
                alias,
                alias.complaint_id == temp_table.complaint_id
                )
            .filter(
                alias.date_received == None,
                temp_table.date_received > actual_date
                )
            )
        column_names = [column.name for column in temp_table.__table__.columns]
        ins_query = (Complaint.__table__.insert()
            .from_select(
                names=column_names,
                select=new_rows
                )
            )
        number_of_new_complaints = new_rows.count()
        self.session.execute(ins_query) 
        self.session.commit()
        return number_of_new_complaints
        
        
    @timer('Search and add deleted data')
    def __find_delete_rows(self, alias):
        sub_q = self.session.query(temp_table.complaint_id)
        alias = aliased(Complaint, alias.subquery())
        non_exist = (self.session
            .query(alias)
            .filter(~alias.complaint_id.in_(sub_q))
            .subquery('non_exist'))

        deleted_rows = (self.session
            .query(
                non_exist.c.complaint_id, 
                non_exist.c.date_received
                )
            .filter(
                Complaint.is_not_deleted_row(non_exist)
                )
            )  
        column_names = ['complaint_id', 'date_received']        
        ins_query = (Complaint.__table__.insert()
            .from_select(
                names=column_names, 
                select=deleted_rows
                )
            )
        number_of_deleted_complaints = deleted_rows.count()
        self.session.execute(ins_query) 
        self.session.commit()
        return number_of_deleted_complaints
        

    def __get_actual_complaints_for_company(self, company):
        latest = (self.session
            .query(
                Complaint.complaint_id,
                func.max(Complaint.update_stamp).label('update_stamp')
                )
            .filter(Complaint.company == company)
            .group_by(Complaint.complaint_id)
            )
        latest_complaints = (self.session
            .query(Complaint)
            .filter(
                and_(
                    Complaint.company == company,    
                    tuple_(
                        Complaint.complaint_id,
                        Complaint.update_stamp
                        )
                    .in_(latest)
                    )
                )
            .subquery(name='latest_complaints')
            )        
        company_complaints = (self.session
            .query(
                latest_complaints.c.date_received, 
                func.count(latest_complaints.c.date_received)
                )
            .filter(
                and_(
                    latest_complaints.c.company == company, 
                    Complaint.is_not_deleted_row(latest_complaints)
                    )
                )
            .group_by(latest_complaints.c.date_received)
            )
        return company_complaints


    @timer('Downloading and filling in the main table')
    def __initial_download_and_filling(self):
        json_data = self.downloader.download_initial_data()
        assert json_data, 'Did not receive records from the database'  
        complaints_to_insert = []
        for json_obj in json_data: 
            assert isinstance(json_obj, dict), 'Invalid format in data'
            complaints_to_insert.append(
                json_obj | {'update_stamp' : AlchDataBase.INITIAL_DATE}
                )
            if len(complaints_to_insert) % 10000 == 0: 
                self.session.execute(
                    Complaint.__table__.insert(),
                    complaints_to_insert
                    )
                self.session.commit()
                complaints_to_insert = []
        if complaints_to_insert:
            self.session.execute(
                Complaint.__table__.insert(),
                complaints_to_insert
                )
            self.session.commit()

