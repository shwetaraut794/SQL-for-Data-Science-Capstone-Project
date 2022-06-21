
""" Importing Libraries """
#from asyncio.windows_events import NULL
#from typing_extensions import Self
from asyncio.windows_events import NULL
import pandas as pd
import numpy as np
import pyodbc
from sqlalchemy import create_engine

""" Connecting database """
Conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
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
    

    def insert_into_NOC_USP(self, NOCRegionData):
        cursor = Conn.cursor()
        
        NOC_id_list=[]
        inserted_data = cursor.execute("select MAX(NOC_id) as MAX_ID from NOC")
        print (inserted_data)
        NOC_id_list=cursor.fetchone()
        print('NOC ID List: ', NOC_id_list[0])
        if (NOC_id_list[0] != None):
            NOC_id = NOC_id_list[0]
        else:
            NOC_id = 0
        NOC_id_list = []
        NOC_Region_df = pd.DataFrame()
        for row in range(NOCRegionData['NOC'].size):
            NOC_id_list.append((row + 1 + NOC_id))
            print(row + 1 + NOC_id)

        NOCRegionData.insert(0, 'NOC_id', NOC_id_list)
        #print(NOCRegionData)
        try:
            #inserted_NOC = pd.read_sql_query('EXEC [dbo].[insert_into_NOC] ?', Conn, params = NOCRegionData)
            inserted_NOC = cursor.execute("EXEC [dbo].[insert_into_NOC] ?", [NOCRegionData]).fetchall()
            Conn.commit()
            print (inserted_NOC)
            print(type(inserted_NOC))
        except Exception as ex:
            print('Exception occured while calling USP')
            print("Exception: [" + type(ex).__name__ + "]", ex.args)
        

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
            inserted_data = cursor.execute("select count(*) from Game  where Year  = '" + Year + "' and Season = '" + Season + "'")

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

    def insert_into_Team(self, sportsData):
        cursor = Conn.cursor()
        id = 1
        """ Creating an empty list for storing id value """
        Team_id_list = []
        inserted_data = cursor.execute("select MAX(Team_id) as MAX_Team_id from Team")
        Team_id_list = cursor.fetchone()
        

        """ if block to calucalte next id value """
        if inserted_data != NULL and Team_id_list[0] != None:
            id = Team_id_list[0] + 1
            print('MAX ID: ', id)
        
        
        for index, row in sportsData.iterrows():
            Teams = str(row['Team'])
            NOC = str(row['NOC'])
            insert_NOC_id =[]
            NOC_id_list=[]
            Find_NOC_id = cursor.execute("Select NOC_id from NOC where NOC = '" + NOC + " '")
            insert_NOC_id = cursor.fetchone()
            print(Teams)


            """ This query is added to ensure that we are not adding same record again in Sport Table """
            tocheck_if_team_present = []
            check_if_team_present = cursor.execute("select count(*) from Team where Teams = '" + Teams + "'")
            tocheck_if_team_present = cursor.fetchone()
            #print(tocheck_if_team_present[0])


            """ This if block is to find out what will be the next noc_id in case NOC is not present in NOC table """
            if insert_NOC_id == None:
                inserted_data = cursor.execute("select MAX(NOC_id) as MAX_ID from NOC")
                NOC_id_list=cursor.fetchone()
                insert_NOC_id[0] = NOC_id_list[0] + 1


            """ We are adding NOC to NOC table if it is present in sportsData but not in NOC table """
            if inserted_data != NULL and len(NOC_id_list) != 0:
                New_NOC_id = NOC_id_list[0] + 1
                insert_query = "insert into NOC (NOC_id, NOC) values (" + str(New_NOC_id)+", '" + NOC + "')"
                cursor.execute(insert_query)
                print("NOCID: " + str(New_NOC_id))
                insert_query = "insert into Team (Team_id, NOC_id, Teams) values (" + str(id) +", '" + str(New_NOC_id) + "' , '" + str(Teams) + "')"
                id += 1
                cursor.execute(insert_query)


            """ This condition is to make sure we are adding only unique records into Team table """    
            if tocheck_if_team_present[0] == 0:
                insert_query = "insert into Team (Team_id, NOC_id, Teams) values (" + str(id) +", '" + str(insert_NOC_id[0]) + "' , '" + Teams + "')"
                cursor.execute(insert_query)
                id += 1
            #print ("Team: " + Teams + "\t|\tNOC: " + NOC)

        Conn.commit()
        """ To check what records are being added into the table NOC """
        """ inserted_data = cursor.execute('select * from Team')
        for i in inserted_data:
            print (i) """



    def insert_into_Event(self,sportsData):
        cursor = Conn.cursor()
        id = 1
        """ Creating an empty list for storing id value """
        E_id_list = []
        inserted_data = cursor.execute("select MAX(E_id) as MAX_E_id from Event")
        E_id_list = cursor.fetchone()

        """ if block to calucalte next id value """
        if inserted_data != NULL and E_id_list[0] != None:
            id = E_id_list[0] + 1
            print(id)
        
        """ For loop to iterate over dataframe sportsData """
        for index, row in sportsData.iterrows():
            Events = str(row['Event'])
            Sports = str(row['Sport'])
            City = str(row['City'])
            insert_Sport_id = []
            find_sport_id = cursor.execute("select sport_id from Sport where Sports  = '" + Sports + "'")
            insert_Sport_id = cursor.fetchone()


            """ This query is added to ensure that we are not adding same record again in Sport Table """
            inserted_data = cursor.execute("select count(*) from Event where Events  = '" + str(Events) + "' ")
            
            """ if block to insert data into Sport table and increment id """
            if cursor.fetchone()[0] == 0:
                insert_query = "insert into Event (E_id, Sport_id_fk, Events, City) values (" + str(id) +", '" + str(insert_Sport_id[0]) + "' , '" + Events + "' , '" + City + "')"
                cursor.execute(insert_query)
                id += 1
        
        Conn.commit()

    

    def insert_into_Athlete(self, sportsData):
        cursor = Conn.cursor()
        """ For loop to iterate over dataframe sportsData """
        for index, row in sportsData.iterrows():
            ID = str(row['ID'])
            Name = str(row['Name'])
            Sex = str(row['Sex'])
            Age = str(row['Age'])
            Weight = str(row['Weight'])
            Height = str(row['Height'])
            Medal = str(row['Medal'])
            Team = str(row['Team'])

            """ This query is added to ensure that we are not adding same record again in Sport Table """
            to_check_name_present =[]
            inserted_data = cursor.execute("select name, A_id from Athlete where name  = '" + str(Name) + "'  and A_id =  " + str(ID) + "")
            to_check_name_present = cursor.fetchone()
            print(to_check_name_present)
            
            """ if block to insert data into Sport table and increment id """
            if to_check_name_present == None:
                
                insert_team_id = []
                find_team_id = cursor.execute("select Team_id from Team where Teams  = '" + Team + "'")
                insert_team_id = cursor.fetchone()
                print(insert_team_id[0])
    
                insert_query = "insert into Athlete (A_id, Team_id_fk, Name, Sex, Age, Weight, Height, Medal) values (" + str(ID) +", '" + str(insert_team_id[0]) + "' , '" + Name + "' , '" + Sex + "',  '" + str(Age) + "',  '" + str(Weight) + "',  '" + str(Height) + "',  '" + Medal + "')"
                cursor.execute(insert_query)
            
                Conn.commit()      



    def insert_into_EventDetail(self, sportsData):
        cursor = Conn.cursor()
        edid = 1
        max_ed_id = []
        inserted_data = cursor.execute("select Max(Ed_id) from EventDetail")
        max_ed_id = cursor.fetchone()

        if inserted_data != NULL and max_ed_id[0] != None:
            print("Max ED ID: ", max_ed_id[0])
            edid = max_ed_id[0] + 1


        """ For loop to iterate over dataframe sportsData """
        for index, row in sportsData.iterrows():
            ID = str(row['ID'])
            Event = str(row['Event'])

            insert_A_id = []
            find_A_id = cursor.execute("select A_id from Athlete where A_id  = '" + ID + "'")
            insert_A_id = cursor.fetchone()
            print(insert_A_id[0])

            insert_E_id = []
            find_E_id = cursor.execute("select E_id from Event where Events  = '" + Event + "'")
            insert_E_id = cursor.fetchone()
            print(insert_E_id[0])

            inserted_data = cursor.execute("select count(*) from EventDetail where A_id_fk  = '" + str(insert_A_id[0]) + "' and E_id_fk =  " + str(insert_E_id[0]) + "")

            if cursor.fetchone()[0] == 0:
                insert_query = "insert into EventDetail (Ed_id, A_id_fk, E_id_fk) values (" + str(edid) +", '" + str(insert_A_id[0]) + "' , '" + str(insert_E_id[0]) + "')"
                cursor.execute(insert_query)
                edid += 1

            
                
            Conn.commit()      


    def insert_into_GameDetail(self, sportsData):
        cursor = Conn.cursor()
        Gamedid = 1
        max_gamed_id = []
        inserted_data = cursor.execute("select Max(gamed_id) from GameDetail")
        max_gamed_id = cursor.fetchone()

        if inserted_data != NULL and max_gamed_id[0] != None:
            print("Max ED ID: ", max_gamed_id[0])
            Gamedid = max_gamed_id[0] + 1


        """ For loop to iterate over dataframe sportsData """
        for index, row in sportsData.iterrows():
            
            Year = str(row['Year'])
            Season = str(row['Season'])
            Event = str(row['Event'])

            insert_Game_id = []
            find_Game_id = cursor.execute("select Game_id from Game where Year  = '" + Year + "' and Season = '" + Season + "'")
            insert_Game_id = cursor.fetchone()
            #print(insert_Game_id[0])

            insert_E_id = []
            find_E_id = cursor.execute("select E_id from Event where Events  = '" + Event + "'")
            insert_E_id = cursor.fetchone()
            #print(insert_E_id[0])

            inserted_data = cursor.execute("select count(*) from GameDetail where Game_id_fk  = '" + str(insert_Game_id[0]) + "' and E_id_fk =  " + str(insert_E_id[0]) + "")

            if cursor.fetchone()[0] == 0:
                insert_query = "insert into GameDetail (Gamed_id, Game_id_fk, E_id_fk) values (" + str(Gamedid) +", '" + str(insert_Game_id[0]) + "' , '" + str(insert_E_id[0]) + "')"
                cursor.execute(insert_query)
                Gamedid += 1

            
                
            Conn.commit() 

        """ function performing all parsing tasks while called """
    def begin_parsing(self):
        cursor = Conn.cursor()
        """ calling read_file function and storing data in SportsStatsData & NOCRegionData """
        sportsData = self.read_main_csv()
        NOCRegionData =  self.read_noc_csv()
        """ Calling function insert_into_NOC """
        #self.insert_into_NOC_USP(NOCRegionData)
        #self.insert_into_NOC(NOCRegionData)
        
        """ Calling function insert_into_Game """
        #self.insert_into_Game(sportsData)
        
        """ Calling function insert_into_Sports """
        #self.insert_into_Sport(sportsData)
        #self.insert_into_Team(sportsData)
        #elf.insert_into_Event(sportsData)
        #self.insert_into_Athlete(sportsData)
        #self.insert_into_EventDetail(sportsData)
        self.insert_into_GameDetail (sportsData)

    

    


""" Creating an instance of class AtheletDataParser """
Athlete = AthleteDataParser()

Athlete.begin_parsing()

