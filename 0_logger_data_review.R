library(tidyverse)
library(data.table)
library(lubridate)
library(skimr)
library(DBI) # needed to connect to data.dfbase
library(dbplyr) # needed to connect to data.dfbase
library(RPostgreSQL) # needed to connect to our data.dfbase
library(rstudioapi) # just so we can type the password as we run the script, so it is not written in the clear
library(tidyverse)
library(lubridate)


##### METHOD DEVELOPMENT SITES - Read in site data for methods development sites ####

#Identify Wesetern databases
myDBs<-c("WMBR_1_1","WEx_SDAM_0","WMBR_2","WMV_1","FD003","FD004") 

junk<-read_csv("https://sdamchecker.sccwrp.org/checker/download/main-all") 
#### MAIN - Read in main site data table and filter for Western sites ####
main_df<- read_csv("https://sdamchecker.sccwrp.org/checker/download/main-all") %>%
  filter(origin_database %in% myDBs) %>%
  transmute( # Align column names to original metric calculator script naming
    Download_date = Sys.time(),
    Database= origin_database,
    ParentGlobalID = globalid,
    SiteCode = sitecode,
    SiteName = sitename,
    Assessors = assessor,
    Recorder= recorder, QA=qa,
    CollectionDate= collectiondate,
    CreationDate= creationdate,
    Creator=creator,
    EditDate=editdate,
    Lat_field= lat, 
    Long_field= long,
    Weather=weathernow,
    PctCloudCover = case_when(is.na(cloudynow) == T ~ 0,T~cloudynow), # Replace missing cloud cover with zero
    rain_yesno = case_when(rain_yesno=="Unk"~NA_character_, T~rain_yesno),
    Disturbances=disturbed,
    Disturbances_details=disturbed_note,
    Bankwidth_0= bankfullwidth0,
    Bankwidth_15= bankfullwidth15,
    Bankwidth_30= bankfullwidth30,
    ReachLength_targ= reachlengthcalc,
    ReachLength_actual= reachlength,
    Slope=valleyslope,
    Landuse=p_landuse,#NESE
    Landuse_s=s_landuse, #Now secondary landuse for NESE. Was primary for other datasets
    Lanuse_Notes=landuse_note,
    Riparian_yn= riparian, 
    ErosionDeposition_yn=erosion,
    FloodplainConnectivity_yn=floodplain,
    SurfaceFlow_pct=hi_reachlengthsurface,
    SurfaceSubsurfaceFlow_pct= case_when(
      is.na(hi_reachlengthsub) ==T ~ SurfaceFlow_pct, # Replace missing subsurface flow with surface flow
      T~hi_reachlengthsub),
    IsolatedPools_number= case_when(
      is.na(pools_observed) == T ~ 0, # Replace missing pool number with zero
      T~pools_observed),
    MaxPoolDepth_calc = max_pool_depth_calc, # NESE
    MaxPoolDepth = max_pool_depth,# NESE
    SeepsSprings_yn=hi_seepsspring,# NESE
    SeepsSprings_inchannel = inchannel,# NESE
    baseflowscore,# NESE
    baseflow_notes,#NESE
    LeafLitter_score = hi_leaflitter,# NESE
    LeafLitter_notes = leafnotes,# NESE
    ODL_score = hi_odl,# NESE
    ODL_notes = odlnotes, # NESE
    WaterInChannel_score = case_when(is.na(hi_channelscore)~2*baseflowscore,
                                     T~hi_channelscore), # CALCULATE DUE TO MISSING FOR NESE (KSM)
    WaterInChannel_notes=  "Calculated as twice the value of NC indicator", # Not recorded for NESE
    HydricSoils_score=hydricsoils,
    HydricSoils_locations=locations, 
    HydricSoils_notes= hydricnotes,
    WoodyJams_number= case_when( # This will consolidate the two woody jams columns for NESE - needs to be fixed in consolidation script still
      is.na(woody_jams) ==T ~ woodyjams1,
      is.na(woodyjams1) ==T ~ woody_jams,
      T~0 # Replace missing wood jams count with zero
    ), 
    # WoodyJams_source=trees_shrubs,  # Not recorded for NESE
    WoodyJams_source=woody_material,  # Same as trees_shrubs in other databases
    # WoodyJams_notes=woodyjamsnotes,# Not recorded for NESE
    UplandRootedPlants_score= uplandrootedplants_score, # Both uplandrootedplants score and fiberousroots score ARE recorded for the NESE region (KSM)
    UplandRootedPlants_notes= uplandrootedplants_notes, 
    FibrousRootedPlants_score= fibrous_rootscore, # Actually, fibrous roots for NESE in ununified database
    FibrousRootedPlants_notes= fibrous_rootnotes, # Actually, fibrous roots for NESE in ununified database
    SedimentOnPlantsDebris_score= hi_debrisscore, 
    SedimentOnPlantsDebris_notes= debrisnotes,
    SoilMoisture1=locationonemoisture, 
    SoilMoisture2=locationtwomoisture,
    SoilMoisture3=locationthreemoisture,
    SoilTexture1=locationonetexture,
    SoilTexture2=locationtwotexture,
    SoilTexture3=locationthreetexture,
    Continuity_score = gi_contbbscore, # NESE - Continuity  of channel and bank score
    Continuity_notes = contbbnotes,# NESE
    Depositional_score = gi_depbbscore,# NESE - Depositional bars and benches
    Depositional_notes = depbbnotes,# NESE
    AlluvialDep_score = gi_radscore,# NESE
    AlluvialDep_notes = radnotes,# NESE
    Headcut_score = gi_headcutscore,# NESE
    Headcut_notes = headcutnotes,# NESE
    GradeControl_score = gi_gcscore,# NESE
    GradeControl_notes = gcnotes,# NESE
    NaturalValley_score = gi_nvscore,# NESE
    NaturalValley_notes = nvnotes, # NESE
    Sinuosity_score= si_sinuosityscore,
    Sinuousity_method= sinuositymethod,
    Sinuosiy_notes=sinuositycondition,
    ActiveFloodplain_score = gi_afpscore,# NESE
    ActiveFloodplain_notes = afpnotes,# NESE
    fp_number,# NESE - number of locations assessed for bankfull width
    fp_bankful,# NESE - Bankfull width, location 1
    fp_floodprone,# NESE - Floodprone width, location 1
    fp_greaterthan,# NESE - 2x bankfull width, location 1
    fp_bankful2,# NESE
    fp_floodprone2,#NESE
    fp_greaterthan2,# NESE
    fp_bankful3,# NESE
    fp_floodprone3,# NESE
    fp_greaterthan3,# NESE
    # ADDED THE CALCULATION OF ENTRENCHMENT SCORE (KSM)
    
    fp_entrenchmentratio1= case_when(fp_greaterthan=="yes"~2.5,T~fp_floodprone/fp_bankful),
    fp_entrenchmentratio2= case_when(fp_greaterthan2=="yes"~2.5,T~fp_floodprone2/fp_bankful2),
    fp_entrenchmentratio3= case_when(fp_greaterthan3=="yes"~2.5,T~fp_floodprone/fp_bankful3),
    ChannelDimensions_method= "direct measurement",# Not recorded for NESE
    RifflePoolSeq_score= gi_sequencescore, 
    RifflePoolSeq_notes= seqnotes,
    SubstrateSorting_score=gi_substratesorting, 
    SubstrateSorting_notes=ginotes, 
    Substrate_Bedrock = scsc_bedrock, #NESE
    Substrate_Boulder = scsc_boulder,#NESE
    Substrate_BoulderSlab = scsc_boulderslab,#NESE
    Substrate_Cobble = scsc_cobble,#NESE
    Substrate_Gravel = scsc_gravel,#NESE
    Substrate_Sand = scsc_sand,#NESE
    Substrate_Silt = scsc_silt,#NESE
    Substrate_Clay = scsc_clay,#NESE
    Substrate_Muck = scsc_muck,#NESE
    Substrate_Leaf = scsc_leaf,#NESE
    Substrate_Fine = scsc_fine,#NESE
    Substrate_Artificial = scsc_artifical,#NESE
    number_of_fish,#NESE
    all_mosqfish,#NESE
    fishabund_note,#NESE
    Fish_score = NA_real_, # Not recorded for NESE
    #BMI_score= abundancescorebenthic,# Not recorded for NESE
    Algae_score= abundancealgae, 
    #Mosquitofish = mosquitofish, # Not recorded for NESE
    #Vertebrate_notes= observedabundancenote, # Not recorded for NESE
    IOFB_yn= observedfungi, 
    #Snakes_yn= observedsnakes, # Not recorded for NESE
    #Snakes_abundance= obsnakesabundance, # Not recorded for NESE
    #Turtles_yn= observedturtles, # Not recorded for NESE
    #Turtles_abundance=obturtlesabundance, # Not recorded for NESE
    #Amphibians_yn=observedamphibians, # Not recorded for NESE
    #Amphibians_abundance= obampiabundance, # Not recorded for NESE
    #FrogVocalizations_yn= observedvocalfrogs, # Not recorded for NESE
    #BiologicalIndicators_notes= observedabundancenote, # Not recorded for NESE
    AlgalCover_Live=streambedlive, 
    AlgalCover_Dead=streambeddead,
    AlgalCover_Upstream=streambeddeadmats,
    AlgalCover_notes= streambedalgaenotes, 
    dens_UU=u_upstream,
    dens_UL=u_left,
    dens_UR=u_right,
    dens_UD=u_downstream,
    dens_MU=m_upstream,
    dens_ML=m_left,
    dens_MR=m_right,
    dens_MD=m_downstream,
    dens_DU=l_upstream,
    dens_DL=l_left,
    dens_DR=l_right,
    dens_DD=l_downstream, 
    Moss_cover=bryophytemosses, 
    Liverwort_cover=bryophyteliverworts,
    Bryophyte_notes=bryophtyenotes,
    ironox_bfscore, #NESE
    ironox_bfnotes,#NESE
    fibrous_rootscore,#NESE
    fibrous_rootnotes,#NESE
    DifferencesInVegetation_score=vegetationdifferencescore, 
    DifferencesInVegetation_notes=vegenotes, 
    NWPL_checklist= regionalindicators, 
    ai_fieldid, #NESE
    Additional_notes= additionalnotes,
    # case_when(#NESE
    #   is.na(ancillarynote) & is.na(additionalnotes) ~ NA_character_,
    #   is.na(ancillarynote) ~additionalnotes, #Other regions have "ancillarynote" field instead of "additionalnotes"
    #   is.na(additionalnotes) ~ ancillarynote),
    hydrovegenote, #NESE
    l1_pendant_id=l1_pendent_id, l1_sampling_round,l1_deployment,l1_deployment_note,l1_time_deployed_checked,l1_water_depth, l1_temp, l1_specific_conductivity, l1_logger_notes,l1_hydro_conditions,l1_hydro_conditions_notes,
    l2_pendant_id=l2_pendent_id, l2_sampling_round,l2_deployment,l2_deployment_note,l2_time_deployed_checked,l2_water_depth, l2_temp, l2_specific_conductivity, l2_logger_notes,l2_hydro_conditions,l2_hydro_conditions_notes
  ) %>%
  rowwise() %>%
  mutate(fp_entrenchmentratio_mean=mean(c(fp_entrenchmentratio1,fp_entrenchmentratio2,fp_entrenchmentratio3), na.rm=T),
         fp_entrenchmentratio_mean=case_when(fp_entrenchmentratio_mean>2.5~2.5, T~fp_entrenchmentratio_mean),
         ChannelDimensions_score=case_when(fp_entrenchmentratio_mean<1.2~0,
                                           fp_entrenchmentratio_mean<2.5~1.5,
                                           fp_entrenchmentratio_mean>=2.5~3,
                                           T~NA_real_)) %>%
  ungroup()



#Bring in logger calibration data
logger_cal<-read_csv("https://sdamchecker.sccwrp.org/checker/download/calibration-all") %>%
  filter(outcome=="Accepted") 

logger_cal %>% group_by(serialnumber) %>% tally() %>% filter(n>1) #Verify that no pendant ID shows up more than once

#Pick a site of interest
my_site<-"AZAW0014"

#Get logger metadata
my_logger_metadata<-main_df %>%
  filter(SiteCode==my_site) %>%
  select(SiteCode,Database, ParentGlobalID, CollectionDate, Assessors, Lat_field, Long_field, starts_with("l1")) %>%
  rename_all(~stringr::str_replace(.,"^l1_","")) %>%
  mutate(LoggerLocation="L1") %>%
  bind_rows(
    main_df %>%
      filter(SiteCode==my_site) %>%
      select(SiteCode,Database, ParentGlobalID, CollectionDate, Assessors, Lat_field, Long_field, starts_with("l2")) %>%
      rename_all(~stringr::str_replace(.,"^l2_","")) %>%
      mutate(LoggerLocation="L2")
  ) %>%
  rename(PendantID = pendant_id) %>%
  #add calibration data
  left_join(logger_cal %>%
              select(PendantID=serialnumber,
                     modalintensity,
                     meanintensity=intensitysubmerged_mean,
                     cal_spcond=spcond_uscm_mean) %>%
              mutate(cutoff = case_when(!is.na(modalintensity) & modalintensity > 0 ~modalintensity,
                                        !is.na(meanintensity) & meanintensity > 0 ~meanintensity,
                                        T~NA_real_)
                     )
            )


#Download all logger data for the site
my_logger_df<-paste0("https://sdamchecker.sccwrp.org/checker/download/logger_raw-",my_site) %>% read_csv()
my_logger_df2<-my_logger_df %>%
  rename(PendantID=login_pendantid) %>%
  mutate(Date = datetime %>% as_date()
  ) %>%
  left_join(    my_logger_metadata %>%
                  select( PendantID, CollectionDate, )) %>%
  filter(!is.na(intensity)) %>%
  mutate(Wet = case_when(!is.na(modalintensity) & modalintensity>0 & intensity > modalintensity~"Wet",
                         !is.na(modalintensity) & modalintensity>0 & intensity <= modalintensity~"Dry",
                         !is.na(modalintensity) & modalintensity<0 & intensity > intensitysubmerged_mean ~"Wet",
                         !is.na(modalintensity) & modalintensity<0 & intensity <= intensitysubmerged_mean ~"Dry",
                         T~"Other"  )
  ) %>%
  arrange()
my_logger_df2 %>% group_by((Wet)) %>% tally()

ggplot(data=my_logger_df2, aes(x=datetime, y=intensity))+
  geom_path(aes(color=login_pendantid %>% as.character()))

main_df %>%
  filter(is.na(Lat_field)) %>% group_by(Database) %>% tally()
