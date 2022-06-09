
""" Importing Libraries """
#from asyncio.windows_events import NULL
#from typing_extensions import Self
from asyncio.windows_events import NULL
import pandas as pd
import numpy as np
import pyodbc

""" Connecting database """
Conn = pyodbc.connect('Driver={SQL Server};'           
                      'Server=DESKTOP-2770KA8;'
                      'Database=SportsStats;'
                      'Trusted_Connection=yes;')

class AthleteDataParser:

    def __init__(self):
        

        """storing data in SportsData """
    def read_main_csv(self):
        SportsData = pd.read_csv('D:\\Shweta\\SQL\\Project\\Data Analysis\\athlete_events.csv')
        return SportsData



    """ reading NOC CSV """
    def read_noc_csv(self):
        NOCData = pd.read_csv('D:\\Shweta\\SQL\\Project\\Data Analysis\\noc_regions.csv')
        return NOCData


    """insert data from NOC csv to NOC table  """
    def insert_into_NOC(self, NOCRegionData):
        cursor = Conn.cursor()
        id = 1
        """ Creating an empty list for storing id value """
        NOC_id_list=[]
        inserted_data = cursor.execute("select MAX(NOC_id) as MAX_ID from NOC")
        #print (inserted_data)
        NOC_id_list=cursor.fetchone()
        #print (NOC_id_list)
        """ If block to check what could be the next value of id """
        if inserted_data != NULL and NOC_id_list[0] != None:
           id = NOC_id_list[0] + 1

        #print ("ID: " + str(id))
        """ For loop to iterate over dataframe NOCRegionData  """    
        for index, row in NOCRegionData.iterrows():
            #print (row['NOC'], row['region'])
            noc = str(row['NOC'])
            region = str(row['region'])

            #print (noc + " | " + region)

            inserted_data = cursor.execute("select count(*) from NOC where NOC = '" + noc + "'")

            if cursor.fetchone()[0] == 0:
                insert_query = "insert into NOC (NOC_Id, NOC, Region) values (" + str(id) + ",'" + noc + "','" + region +  "')"
                cursor.execute(insert_query)
                id += 1

        Conn.commit()
        """ To check what records are being added into the table NOC """
        """ inserted_data = cursor.execute('select * from NOC')
        for i in inserted_data:
            print (i) """
    

    """ inserting data from main csv into Game table  """
    def insert_into_Game(self, sportsData):
        cursor = Conn.cursor()
        id = 1
        """ Creating an empty list for storing id value """
        Game_id_list = []
        inserted_data = cursor.execute("select MAX(Game_id) as MAX_Game_id from Game")
        Game_id_list = cursor.fetchone()

        """ if block to calucalte next id value """
        if inserted_data != NULL and Game_id_list[0] != None:
            id = Game_id_list[0] + 1
            #print(id)  
        
        """ For loop to iterate over dataframe sportsData """
        for index, row in sportsData.iterrows():
            Year = str(row['Year'])
            Season = str(row['Season'])

            """ This query is added to ensure that we are not adding same record again in Game Table """
            inserted_data = cursor.execute("select count(*) from Game where Year = " + Year + "")

            """ if block to insert data into Game table and increment id """
            if cursor.fetchone()[0] == 0:
                insert_query = "insert into Game (Game_id, Year, Season) values (" + str(id) +", '" + Year + "' , '" + Season + "')"
                cursor.execute(insert_query)
                id += 1

        Conn.commit()



    """ inserting data from main csv into Sport table  """    
    def insert_into_Sport(self, sportsData):
        cursor = Conn.cursor()
        id = 1
        """ Creating an empty list for storing id value """
        sport_id_list = []
        inserted_data = cursor.execute("select MAX(Sport_id) as MAX_Sport_id from Sport")
        sport_id_list = cursor.fetchone()

        """ if block to calucalte next id value """
        if inserted_data != NULL and sport_id_list[0] != None:
            id = sport_id_list[0] + 1
            print(id)
        
        """ For loop to iterate over dataframe sportsData """
        for index, row in sportsData.iterrows():
            Sports = str(row['Sport'])

            """ This query is added to ensure that we are not adding same record again in Sport Table """
            inserted_data = cursor.execute("select count(*) from Sport where Sports = '" + Sports + "'")
            
            """ if block to insert data into Sport table and increment id """
            if cursor.fetchone()[0] == 0:
                insert_query = "insert into Sport (Sport_id, Sports) values (" + str(id) +", '" + Sports +"')"
                cursor.execute(insert_query)
                id += 1
        
        Conn.commit()


        """ function performing all parsing tasks while called """
    def begin_parsing(self):
        cursor = Conn.cursor()
        """ calling read_file function and storing data in SportsStatsData & NOCRegionData """
        sportsData = self.read_main_csv()
        NOCRegionData =  self.read_noc_csv()
        """ Calling function insert_into_NOC """
        self.insert_into_NOC(NOCRegionData)
        """ Calling function insert_into_Game """
        self.insert_into_Game(sportsData)
        """ Calling function insert_into_Sports """
        self.insert_into_Sport(sportsData)
       

    

    


""" Creating an instance of class AtheletDataParser """
Athlete = AthleteDataParser()

Athlete.begin_parsing()

