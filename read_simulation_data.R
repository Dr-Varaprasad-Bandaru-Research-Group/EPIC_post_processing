
#Load the required libraries.
library(sp)
library(rgdal)
library(reshape2) 
library(raster)
library(parallel)
library(readr)
require(ncdf4)


create_database = function(input){
  
  #Read the input such as ensemble number, month, year, type of crop etc.   
  ensemble_str = vec[1]
  ensemble = as.numeric(ensemble_str)
  months = vec[2]
  year = vec[3]
  crop_type = input[4]
  model = input[5]
  
  #Choose the maximum cores you chose for the simulations.
  n_cores = 70
  
  #database columns are number of simulation years + 1.
  database <- data.frame(matrix(ncol = 11, nrow = 0))
  
  #Name the database columns.
  year_numeric = as.numeric(year)
  col_names = c("pixel_id", (year_numeric - 9 ): year_numeric)
  colnames(database) = col_names
  
  #Standard width settings required for reading the yield from EPIC output.
  widths = c(5, 5, 5, 9, rep(9, times = 34), 3)
  
  
  for(core in 1 : n_cores){
    
    print(paste0("Starting with core: ", core, " month: ", month, " year: ",year," crop_type: ",crop_type," ensemble: ", ensemble))
    
    inpath = paste0("Path/to/temp.", core, "/")
    
    if(dir.exists(inpath)){
      
      #list all the files exsting in the core folder.
      files = list.files(path = inpath, full.names = FALSE, pattern = "*.ACY")
      pixel_ids = gsub(pattern = "*.ACY", replacement = "", x = files)
      
      #Make the temporary database for reading all the files in this core directory. 
      database_temp = data.frame(matrix(ncol = 11, nrow = length(pixel_ids)))
      colnames(database_temp) = col_names
      database_temp$pixel_id = pixel_ids
      
      
      nrows = nrow(database_temp)
      start_time = Sys.time()
      
      for(row in 1 : nrows){
        
        yield_file = paste0(inpath, database_temp[row, 1], ".ACY")
        file_info = file.info(yield_file)
        
        #Read the file if it exists.
        yields = tryCatch(
          {
            
            data = read_fwf(file = yield_file, col_positions = fwf_widths(widths), col_types = cols_only(X4 = col_character()), skip = 11)
            yield_vec = as.numeric(data$X4)
            
          },
          
          error=function(cond){
            return(NA)
          }
          
        )
        
        #If file is read successfully then store each year's yield separately.  
        if(!anyNA(yields) && length(yields) == 10){
          
          for(index in 1 : 10){
            database_temp[row, index + 1] = yield_vec[index]
          }
          
        }
        
      }
      
      #Once all the files in current directory are read then append temp database to the main database.
      database = rbind(database, database_temp)
      
    }
  }
  
  
  #set the outpath
  outpath = paste0("/path/to/store/file/")
  if (!dir.exists(outpath)){
    dir.create(outpath, recursive = TRUE)  
  }
  
  filename = paste0("filename.csv")
  write.table(x = database, file = paste0(outpath, filename), sep = ",", row.names = FALSE)
  print(paste0("Postprocessing is complete !"))
    
}


ensemble_range = 1 : 15
months = c("06", "07", "08", "09")
years = c("2014")
selections = list()
counter = 1
for (year in years){
  for (month in months){
    for(ensemble in ensemble_range){
      selections[[counter]] = c(ensemble, month, year)
      counter = counter + 1
    }
  }  
}


dat = mclapply(X = selections ,FUN = create_database, mc.cores = length(selections))
#create_database(c("1", "09", "2012"))








