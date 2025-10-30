import pandas as pd
import folium
import os
import seaborn as sns
import re
from IPython.display import display
import webbrowser
from folium.plugins import MarkerCluster
import pyproj
from folium.plugins import MousePosition


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
    
    
df_call_parameters.info()




df_call_parameters_dateRange = df_call_parameters[(df_call_parameters['startRange'] == '2025-05-15T09:00:00Z') & (df_call_parameters['endRange'] == '2025-10-14T23:59:59Z')]    

df_call_parameters_dateRange = df_call_parameters[(df_call_parameters['startRange'] == '2025-10-15T00:00:00Z') & (df_call_parameters['endRange'] == '2025-10-15T23:59:59Z')]    




df_call_parameters_dateRange_aiport_ori = df_call_parameters_dateRange[['origin_continent','origin_country','origin_latitude','origin_longitude','origin','origin_airport','totalFlights',"totalPages"]]
df_call_parameters_dateRange_aiport_ori.columns = ['continent','country','latitude','longitude',"iata",'airport', "totalFlights","totalPages"]

df_call_parameters_dateRange_aiport_dest = df_call_parameters_dateRange[['destination_continent','destination_country','destination_latitude','destination_longitude','destination','destination_airport','totalFlights',"totalPages"]]
df_call_parameters_dateRange_aiport_dest.columns = ['continent','country','latitude','longitude',"iata",'airport', "totalFlights","totalPages"]




df_call_parameters_dateRange_aiport = pd.concat([df_call_parameters_dateRange_aiport_ori,df_call_parameters_dateRange_aiport_dest]).groupby(['continent','country','latitude','longitude',"iata",'airport']).sum().reset_index()




df_call_parameters_dateRange_aiport = df_call_parameters_dateRange_aiport.sort_values(['totalFlights'],ascending=False).reset_index().drop(['index'], axis=1)


df_call_parameters_dateRange_aiport['airport_desc'] = df_call_parameters_dateRange_aiport['country'] +  "<br>" + df_call_parameters_dateRange_aiport['iata'] +  "<br>" +  df_call_parameters_dateRange_aiport['airport'] 


df_call_parameters_dateRange_aiport.info()



# Create a map
m = folium.Map(crs='EPSG3857',
               location=[50,15], zoom_start=2,

               min_zoom = 2,max_bounds=True)


marker_cluster = MarkerCluster(name = 'Airport',overlay =True)

# Add marker_cluster to current site_map
m.add_child(marker_cluster)

# for each row in spacex_df data frame
# create a Marker object with its coordinate
# and customize the Marker's icon property to indicate if this launch was successed or failed, 
# e.g., icon=folium.Icon(color='white', icon_color=row['marker_color']
for index, record in df_call_parameters_dateRange_aiport.iterrows():
    # TODO: Create and add a Marker cluster to the site map
    marker = folium.Marker(location=[df_call_parameters_dateRange_aiport.iloc[index]['latitude'],
                                     df_call_parameters_dateRange_aiport.iloc[index]['longitude']],
                           popup = folium.Popup(df_call_parameters_dateRange_aiport.iloc[index]['airport_desc'], max_width=300),

                           icon=folium.Icon(color='white', icon_color='red'))
    marker_cluster.add_child(marker)







df_call_parameters_sum = df_call_parameters.dropna(subset=['origin','destination'])
df_call_parameters_sum['origin_reordered'] = df_call_parameters_sum[['origin','destination']].min(axis=1)
df_call_parameters_sum['dest_reordered'] = df_call_parameters_sum[['origin','destination']].max(axis=1)

df_call_parameters_sum = df_call_parameters_sum[['origin_reordered','dest_reordered','totalFlights']].groupby(['origin_reordered','dest_reordered']).sum().reset_index()

df_call_parameters_sum.columns = ['origin','destination','totalFlights']

df_call_parameters_sum = df_call_parameters_sum.merge(df_airports_country_origin).merge(df_airports_country_destination).replace('',None)
    
df_call_parameters_sum['itinerary'] =  df_call_parameters_sum.origin_country + " - " +df_call_parameters_sum.origin + " - " + df_call_parameters_sum.origin_airport + " <-> " +  df_call_parameters_sum.destination_country + " - "+ df_call_parameters_sum.destination + " - " + df_call_parameters_sum.destination_airport + "<br>Nb flights = " + df_call_parameters_sum.totalFlights.astype('str')

df_call_parameters_sum = df_call_parameters_sum[df_call_parameters_sum['totalFlights'] > 0]

gc = folium.FeatureGroup(name="Great circles", show=True)

    
    
    

for startlat,startlong, endlat,  endlong , n, itinerary in zip(
    df_call_parameters_sum.origin_latitude,
    df_call_parameters_sum.origin_longitude,
    df_call_parameters_sum.destination_latitude, 
    df_call_parameters_sum.destination_longitude,
    df_call_parameters_sum.totalFlights,
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
    


    # calculate line string along path with segments <= 1 km
    lonlats = g.npts(lon1 = startlong,
                    lat1 = startlat,
                    lon2 = endlong,
                    lat2 = endlat,
                    npts = 1 + int(dist / 1000),
                    initial_idx = 0,
                    terminus_idx = 0)



    lonlats = [[val[1],val[0]+360]  if ((val[0] <0) & (((endlong - startlong) > 180)|((endlong - startlong) < -180))) else [val[1],val[0]]  for val in lonlats ]
    

    
    # Plot Great circles
    

    
    folium.PolyLine(lonlats, color='red',popup=folium.Popup(itinerary , max_width=4000), weight=1).add_to(gc)


# Add layers to the map


gc.add_to(m)
folium.LayerControl().add_to(m)           



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
webbrowser.open("map.html")


# Check https://github.com/ghybs/Leaflet.FeatureGroup.SubGroup