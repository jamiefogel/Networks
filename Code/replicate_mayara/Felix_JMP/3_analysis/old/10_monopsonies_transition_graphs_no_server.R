#install.packages("plotly")
#install.packages("Hmisc")
#install.packages("reshape")
#install.packages("dplyr")
library(plotly)
library(Hmisc)
library(reshape)
library(dplyr)

# clear everything
rm(list = ls())

############ Set parameters ############ 

# Path
inpath  <-"/Users/mayara/Dropbox (MIT)/research/Projects/Monopsonies/code/input/transitions/20210802"
outpath <-"/Users/mayara/Dropbox (MIT)/research/Projects/Monopsonies/code/output/20210802"

######### Input directory #############
setwd(inpath)

# List files to be imported
infiles <- list.files(".")
n <- length(infiles)

# Names of R files
fnames <-gsub("for_R_transition_","",infiles)
fnames <-gsub(".csv","",fnames)

# Store var type, cohort year, out year in separate lists
tvar   <- sapply(strsplit(fnames, '_'), '[', 1) 
cohort <- sapply(strsplit(fnames, '_'), '[', 2) 
outy   <- sapply(strsplit(fnames, '_'), '[', 3)

# Import and reshape dataset into long

alltrans <- c()
for (i in seq(1,n, by=1)) {
  
  print(paste("Importing ",infiles[[i]],sep=" "))
  templong <- read.csv(infiles[[i]])
  
  templong <- select(templong,c(origin, dest, flow, origin_label, 
                                dest_label,
                                PCT, PCTstay, origin_order, 
                                dest_order,
                                origin_label_PCTstay, origin_label_PCT,
                                diag_PCTstay, diag_PCTstaytop10, 
                                diag_PCTstaytop50))

  # Keep track of transition variable, cohort, and transition year out
  templong$transvar <- tvar[i]
  templong$cohort   <- cohort[i]
  templong$outyear  <- outy[i]
  
  # Append all into one large dataframe of transition
  alltrans <-rbind(alltrans,templong)
  rm("templong")
}

############ Output directory ############ 
# Set working directory to graph output folder 
setwd(outpath)

# Legend line style
lstyle <-list(font = list(color="black"))

################# Plain heatmap ####################

### Loop through PCT variables we wish to plot

# Need to do this manually one at a time because savegraphs is not working - server error problem
# transvars: mmc, cbo942d, cnae952d, llmtop50
# cutplots: 50

pctvars   <-"PCTstay"
top10vals <-50
transvars <-"llmtop50" 
cohorts   <-1990
outyears  <- 1991

for (cutplot in top10vals){
for (transplot in transvars){
  for (cohortplot in cohorts){
    for (outyearplot in outyears){
      for (pctvar in pctvars){
        
        TRANS <-alltrans[which(alltrans$cohort   == cohortplot &
                               alltrans$transvar == transplot    &
                               alltrans$outyear  == outyearplot  ),]
   
        if (cutplot>0){
          TRANS <-TRANS[which(TRANS$origin_order   <= cutplot &
                              TRANS$dest_order   <= cutplot  ),]
        }
    
        if (transplot=="mmc"){
          transptype <-"microregion"
        }
        else if (transplot=="cbo942d"){
          transptype <-"occupation"
        }
        else if (transplot=="cnae952d"){
          transptype <-"sector"
        }
        else if (transplot=="llmtop50"){
          transptype <-"LLM"
        }
        
        if (pctvar=="PCTstay"){
          TRANS <-TRANS[which(TRANS$dest_label!="Exit"),]
          TRANSsub <- select(TRANS,c(dest_order,origin_order,all_of(pctvar),origin_label_PCTstay,dest_label))
        }
        else{
          TRANSsub <- select(TRANS,c(dest_order,origin_order,all_of(pctvar),origin_label,dest_label))
        }
        
        if (cutplot==0){
          pct_diag = 100*round(unique(TRANS$diag_PCTstay),2)
        }
        if (cutplot==10){
          pct_diag = 100*round(unique(TRANS$diag_PCTstaytop10),2)
        }
        if (cutplot==50){
          pct_diag = 100*round(unique(TRANS$diag_PCTstaytop50),2)
        }
        
        names(TRANSsub) <-c("dest","origin","plot","origin_label","dest_label")
        
        y_n<-length(unique(TRANSsub$origin))
        x_n<-length(unique(TRANSsub$dest))
        
        text_x <- x_n/2 +0.6
        text_y <- y_n/2 +0.6
        xfontsize <- 10 + (cutplot>0)*2
        yfontsize <- 8 + (cutplot>0)*4
       
        # Plot if there is any data to be plot in combo of desired locals
        nmiss <-nrow(TRANS)
        if (nmiss>1){
          
          # Plain heatmap
          myplot <-plot_ly(type="heatmap",
                      x = TRANSsub$dest,
                      y = TRANSsub$origin_label,
                      z = TRANSsub$plot,
                      colorscale="Portland") %>%
            layout(xaxis = list(title = paste('Destination',transptype,sep=" "),
                                showgrid=FALSE,
                                zeroline=FALSE,
                                tickvals=seq(0,x_n,by=1),
                                tickfont = list(size = xfontsize),
                                tickangle=-65,
                                size=0.5,
                                cex = 0.75,
                                side="bottom",
                                color="black"),
                   yaxis = list(title = '',
                                showgrid=FALSE,
                                zeroline=FALSE,
                                tickvals=seq(0,y_n,by=1),
                                tickfont = list(size = yfontsize),
                                categoryorder = "category descending",
                                label=0.5,
                                size=1,
                                cex = 0.75,
                                side="left",
                                color="black"),
                   annotations= list(x=text_x,
                                     y=text_y,
                                     align="center",
                                     valign="top",
                                     text=paste(pct_diag,'%',' of firm switchers \n are on diagonal \n(do not switch ',transptype,')',sep=""),
                                     font = list(
                                       size = 12,
                                       color = toRGB("white") ),
                                     xanchor="left",
                                     yanchor="middle",
                                     showarrow = TRUE,
                                     arrowcolor = toRGB("white"),
                                     arrowhead = 1,
                                     ax = 100,
                                     ay = -50
                                   ),
                   margin=list(l=75,b=75)
            ) %>%
            colorbar(title = "% from\norigin",
                     titlefont=list(color="black"),
                     limits=c(0,100),
                     tickvals=c(0,25,50,75,100),
                     tickfont=list(color="black"),
                     height=10,
                     width=2)
          # Save on viewer
          myplot
        } # Any non-missing rows to plot boolean
      } # Close pctvars
    } # Close outyears
  } # Close cohorts
} # transvars
} # Close top 10 or not
