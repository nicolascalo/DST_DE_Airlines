import pandas as pd
import folium
import os
import seaborn as sns
import re
from IPython.display import display
import webbrowser
from folium.plugins import MarkerCluster
from folium.plugins import FeatureGroupSubGroup

import pyproj
from folium.plugins import MousePosition
import datetime
import numpy as np


### Setting up working directory


if  bool(re.search("DST_DE_Airlines$",os.getcwd())) == True:
    os.chdir("1_data_collection/afklm_api_collection") 


if  bool(re.search("1_data_collection$",os.getcwd())) == True:
    os.chdir("afklm_api_collection") 


if  bool(re.search("ongoing_work$",os.getcwd())) == True:
    os.chdir("..") 


os.getcwd()



path_call_parameter_file_folder = "call_parameter_lists"
path_call_parameter_csv_root = "df_call_parameters"
remove_loop_from_to = True
plot_folder = 'EDA_plots'




df_airports = pd.read_csv("../df_iata_icao_wiki_final_world.csv").fillna('')
df_airports.info()



path_call_parameter_csv_list = os.listdir(path_call_parameter_file_folder)

call_parameter_csv_list = [val for val in path_call_parameter_csv_list if 'df_call_parameters'  in val]

df_call_parameters = pd.DataFrame()



for call_parameter_csv in call_parameter_csv_list :
    
    df_call_parameters_to_add = pd.read_csv(path_call_parameter_file_folder +"/"+call_parameter_csv).fillna('')

    df_call_parameters = pd.concat([df_call_parameters, df_call_parameters_to_add],ignore_index=True).fillna('').sort_values(['startRange','endRange'])





df_call_parameters = df_call_parameters[df_call_parameters['origin'] != df_call_parameters['destination']]    


df_airports.info()

df_airports_country_origin = df_airports[['continent','subcontinent','country','iata','airport','latitude','longitude']] 
df_airports_country_origin.columns = ['origin_continent','origin_subcontinent',"origin_country", "origin", "origin_airport",'origin_latitude','origin_longitude']
df_airports_country_destination = df_airports[['continent','subcontinent','country','iata','airport','latitude','longitude']] 
df_airports_country_destination.columns = ['destination_continent','destination_subcontinent',"destination_country", "destination", "destination_airport",'destination_latitude','destination_longitude']


df_call_parameters = df_call_parameters.merge(df_airports_country_origin).merge(df_airports_country_destination).replace('',None)
    
    
#df_call_parameters['destination_subcontinent'] = "-  " + df_call_parameters['destination_subcontinent'] 
#df_call_parameters['origin_subcontinent'] = "-  " + df_call_parameters['origin_subcontinent'] 
        
df_call_parameters.info()



df_call_parameters['dailyFlights'] = df_call_parameters.apply(lambda row: None if row['totalFlights'] is None  else  int(row['totalFlights'] / (datetime.datetime.fromisoformat(row['endRange'])-datetime.datetime.fromisoformat(row['startRange'])).days ) , axis=1)




df_call_parameters_aiport_ori = df_call_parameters[['origin_continent','origin_subcontinent',"origin_country",'origin_latitude','origin_longitude','origin','origin_airport','totalFlights',"totalPages",'dailyFlights']]
df_call_parameters_aiport_ori.columns = ['continent','subcontinent','country','latitude','longitude',"iata",'airport', "totalFlights","totalPages",'dailyFlights']

df_call_parameters_aiport_dest = df_call_parameters[['destination_continent','destination_subcontinent',"destination_country",'destination_latitude','destination_longitude','destination','destination_airport','totalFlights',"totalPages",'dailyFlights']]
df_call_parameters_aiport_dest.columns = ['continent','subcontinent','country','latitude','longitude',"iata",'airport', "totalFlights","totalPages",'dailyFlights']








df_call_parameters_aiport = pd.concat([df_call_parameters_aiport_ori,df_call_parameters_aiport_dest]).groupby(['continent','subcontinent','country','latitude','longitude',"iata",'airport']).sum().reset_index()




df_call_parameters_aiport = df_call_parameters_aiport.sort_values(['dailyFlights'],ascending=False).reset_index().drop(['index'], axis=1)




df_call_parameters_aiport['airport_desc'] = df_call_parameters_aiport['country'] +  "<br>" + df_call_parameters_aiport['iata'] +  "<br>" +  df_call_parameters_aiport['airport'] 


connection_list = []

for index, record in  df_call_parameters_aiport.iterrows():
    df = df_call_parameters[df_call_parameters['dailyFlights'] > 0].reset_index().copy(deep=True)
    
    df = df[(df['origin'] == record.iata) |(df['destination'] == record.iata)]
    
    
    
    connection_iata = set(df['origin'].to_list() + df['destination'].to_list() )
    
    df_aiport_filtered = df_call_parameters_aiport[df_call_parameters_aiport['iata'].isin(connection_iata)].copy(deep=True)
    df_aiport_filtered = df_aiport_filtered[df_aiport_filtered['iata'] != record.iata]
    df_aiport_filtered['connections'] = df_aiport_filtered['continent'] +  " - " + df_aiport_filtered['subcontinent'] +  " - " + df_aiport_filtered['country'] +  " - " + df_aiport_filtered['iata'] +  " - " +  df_aiport_filtered['airport'] 
    df_aiport_filtered = df_aiport_filtered.sort_values('connections')
    connections = "<br>".join(df_aiport_filtered['connections'].to_list())
    
    connection_list.append(connections)
    
df_call_parameters_aiport['airport_desc'] =   df_call_parameters_aiport['airport_desc'] + "<br><br>Connections to:<br>"  + connection_list


df_call_parameters_aiport.info()


df_call_parameters_valid_airports = set(df_call_parameters[df_call_parameters['dailyFlights'] > 0][['origin','destination']].to_numpy().flatten())

df_call_parameters_aiport = df_call_parameters_aiport[(df_call_parameters_aiport['iata'].isin( df_call_parameters_valid_airports))]






#df_call_parameters = df_call_parameters[(df_call_parameters['origin_continent'] == 'Asia') & (df_call_parameters['destination_continent'] == 'Asia')]    


df_call_parameters.info()




# Create a map
m = folium.Map(crs='EPSG3857',
               location=[50,15], zoom_start=2,

               min_zoom = 2,max_bounds=True)




# Create a map
m = folium.Map(crs='EPSG3857',
               location=[50,15], zoom_start=2,

               min_zoom = 2,max_bounds=True)


marker_cluster = MarkerCluster(name = 'Airport',overlay =True, control=False)


m.add_child(marker_cluster)


for index, record in df_call_parameters_aiport.iterrows():
    # TODO: Create and add a Marker cluster to the site map
    marker = folium.Marker(location=[df_call_parameters_aiport.iloc[index]['latitude'],
                                     df_call_parameters_aiport.iloc[index]['longitude']],
                           popup = folium.Popup(df_call_parameters_aiport.iloc[index]['airport_desc'], max_width=3000),

                           icon=folium.Icon(color='white', icon_color='red'))
    marker_cluster.add_child(marker)





location_airports_intra = {}
location_airports_inter = {}

intra = MarkerCluster(name = "Intra-region",overlay =True)

for continent in set(df_call_parameters_aiport['continent']):
    continent_mod = "--- Intra " + continent
    key = continent_mod
    value_continent = folium.plugins.FeatureGroupSubGroup(intra, continent_mod)
    location_airports_intra[key] =  value_continent 
    for subcontinent in set(df_call_parameters_aiport[df_call_parameters_aiport['continent'] == continent]['subcontinent']):
        subcontinent = "-------- Intra " + subcontinent
        key =  subcontinent
        value_subcontinent = folium.plugins.FeatureGroupSubGroup(value_continent, subcontinent)
        location_airports_intra[key] =  value_subcontinent

inter = MarkerCluster(name = "Inter-region",overlay =True)

for continent in set(df_call_parameters_aiport['continent']):
    continent_mod = "--- Inter " + continent
    key = continent_mod
    value_continent = folium.plugins.FeatureGroupSubGroup(inter, continent_mod)
    location_airports_inter[key] =  value_continent 
    for subcontinent in set(df_call_parameters_aiport[df_call_parameters_aiport['continent'] == continent]['subcontinent']):
        subcontinent = "-------- Inter " + subcontinent
        key =  subcontinent
        value_subcontinent = folium.plugins.FeatureGroupSubGroup(value_continent, subcontinent)
        location_airports_inter[key] =  value_subcontinent


df_call_parameters_sum = df_call_parameters.dropna(subset=['origin','destination'])
df_call_parameters_sum['origin_reordered'] = df_call_parameters_sum[['origin','destination']].min(axis=1)
df_call_parameters_sum['dest_reordered'] = df_call_parameters_sum[['origin','destination']].max(axis=1)

df_call_parameters_sum = df_call_parameters_sum[['origin_reordered','dest_reordered','totalFlights','dailyFlights']].groupby(['origin_reordered','dest_reordered']).sum().reset_index()

df_call_parameters_sum.columns = ['origin','destination','totalFlights','dailyFlights']

df_call_parameters_sum = df_call_parameters_sum.merge(df_airports_country_origin).merge(df_airports_country_destination).replace('',None)
    
df_call_parameters_sum['itinerary'] =  df_call_parameters_sum.origin_country + " - " +df_call_parameters_sum.origin + " - " + df_call_parameters_sum.origin_airport + " <-> " +  df_call_parameters_sum.destination_country + " - "+ df_call_parameters_sum.destination + " - " + df_call_parameters_sum.destination_airport + "<br>totalFlights = " + df_call_parameters_sum.totalFlights.astype('str')+ "<br>dailyFlights = " + df_call_parameters_sum.dailyFlights.astype('str')

df_call_parameters_sum = df_call_parameters_sum[df_call_parameters_sum['dailyFlights'] > 0]


#df_call_parameters_sum['destination_subcontinent'] = "-  " + df_call_parameters_sum['destination_subcontinent'] 
#df_call_parameters_sum['origin_subcontinent'] = "-  " + df_call_parameters_sum['origin_subcontinent'] 


#df_call_parameters_sum = df_call_parameters_sum[(df_call_parameters_sum['origin_continent'] == 'Asia') ]   


max_n = max(df_call_parameters_sum.dailyFlights)
min_n = min(df_call_parameters_sum.dailyFlights)
palette = sns.color_palette("plasma",
                            
                            n_colors = int(np.log2(max_n - min_n + 1))).as_hex()
palette_2 = reversed(palette)
palette_2 = palette
dict_n = dict(enumerate(palette_2, start = int(min_n)))



max_dailyFlights = df_call_parameters_sum['dailyFlights'].max()

df_call_parameters_sum.sort_values('dailyFlights',ascending=False)

for origin_subcontinent, destination_subcontinent, startlat,startlong, endlat,  endlong , totalFlights,dailyFlights, itinerary in zip(
    df_call_parameters_sum.origin_subcontinent,
    df_call_parameters_sum.destination_subcontinent, 
    df_call_parameters_sum.origin_latitude,
    df_call_parameters_sum.origin_longitude,
    df_call_parameters_sum.destination_latitude, 
    df_call_parameters_sum.destination_longitude,
    df_call_parameters_sum.totalFlights,
    df_call_parameters_sum.dailyFlights,
    df_call_parameters_sum.itinerary ):
    


    # calculate distance between points
    g = pyproj.Geod(ellps='WGS84')
    
    
    print("")

    print(itinerary)
    print(f"{startlong = }")
    print(f"{startlat = }")   
    print(f"{endlong = }")   
    print(f"{endlat = }")     
    print(f"{endlong - startlong = }")  


    (az12, az21, dist) = g.inv(startlong, startlat, endlong, endlat)
    


    # calculate line string along path with segments <= 1 kmf
    lonlats = g.npts(lon1 = startlong,
                    lat1 = startlat,
                    lon2 = endlong,
                    lat2 = endlat,
                    npts = 1 + int(dist / 1000),
                    initial_idx = 0,
                    terminus_idx = 0)



    lonlats = [[val[1],val[0]+360]  if ((val[0] <0) & (((endlong - startlong) > 180)|((endlong - startlong) < -180))) else [val[1],val[0]]  for val in lonlats ]
    

    
    # Plot Great circles
    opacity=dailyFlights/(max_dailyFlights)
    

    opacity = np.log2(dailyFlights) /np.log2(max_n)  
    
    
    # folium.PolyLine(lonlats, color=dict_n.get(dailyFlights),popup=folium.Popup(itinerary , max_width=4000), weight=2.5).add_to(gc)
    polyline = folium.PolyLine(lonlats,
                               color=dict_n.get(int(np.log2(dailyFlights))),
                               opacity = opacity,
                               popup=folium.Popup(itinerary , max_width=4000),
                               weight=2.5)
    
    polyline2 = folium.PolyLine(lonlats,
                                color=dict_n.get(int(np.log2(dailyFlights))),
                                opacity=opacity,
                                popup=folium.Popup(itinerary , max_width=4000),
                                weight=2.5)
    
    
    if origin_subcontinent == destination_subcontinent:
        polyline.add_to(location_airports_intra["-------- Intra " + destination_subcontinent])
        polyline2.add_to(location_airports_intra["-------- Intra " + origin_subcontinent])
    else:
        polyline.add_to(location_airports_inter["-------- Inter " + destination_subcontinent])
        polyline2.add_to(location_airports_inter["-------- Inter " + origin_subcontinent])



m.add_child(intra)
for key in location_airports_intra:
    m.add_child(location_airports_intra[key])
m.add_child(inter)
for key in location_airports_inter:
    m.add_child(location_airports_inter[key])

#m.add_child(gc)




# Add layers to the map


folium.LayerControl(collapsed=False).add_to(m)    


formatter = "function(num) {return L.Util.formatNum(num, 5);};"
mouse_position = MousePosition(
    position='topright',
    separator=' Long: ',
    empty_string='NaN',
    lng_first=False,
    num_digits=20,
    prefix='Lat:',
    lat_formatter=formatter,
    lng_formatter=formatter,
)

m.add_child(mouse_position)

m.save("folium_map.html")
#webbrowser.open("folium_map.html")


# Check https://github.com/ghybs/Leaflet.FeatureGroup.SubGroup