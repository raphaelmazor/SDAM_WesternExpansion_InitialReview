myDBs<-c("WMBR_1_1","WEx_SDAM_0","WMBR_2","WMV_1","FD003","FD004") 

#### MAIN - Read in main site data table and filter for GP sites ####
junk<-read_csv("https://sdamchecker.sccwrp.org/checker/download/main-all") 
main_df<- read_csv("https://sdamchecker.sccwrp.org/checker/download/main-all") %>%
  # junk%>%
  # filter(origin_database %in% myDBs) %>%
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
    hydrovegenote #NESE
  ) %>%
  rowwise() %>%
  mutate(fp_entrenchmentratio_mean=mean(c(fp_entrenchmentratio1,fp_entrenchmentratio2,fp_entrenchmentratio3), na.rm=T),
         fp_entrenchmentratio_mean=case_when(fp_entrenchmentratio_mean>2.5~2.5, T~fp_entrenchmentratio_mean),
         ChannelDimensions_score=case_when(fp_entrenchmentratio_mean<1.2~0,
                                           fp_entrenchmentratio_mean<2.5~1.5,
                                           fp_entrenchmentratio_mean>=2.5~3,
                                           T~NA_real_)) %>%
  ungroup()

main_df %>%
  select(SiteCode) %>%
  unique() %>%
  arrange(SiteCode) %>%
  write.table(file="clipboard", sep="\t", row.names = F)

aw_xwalk
wm_xwalk
nese_xwalk
gp_xwalk

combined_xwalk<-
  main_df %>% select(SiteCode) %>% unique() %>%
  left_join(
    bind_rows(
      aw_xwalk %>%
        select(SiteCode=SITECODE,Class=Determination_Final) %>%
        unique(),
      wm_xwalk %>%
        select(SiteCode=SITECODE, Class=Determination_Final) %>%
        unique(),
      nese_xwalk %>%
        select(SiteCode=SiteCode, Class=Determination_final) %>%
        unique(),
      gp_xwalk %>%
        select(SiteCode=Site.Code, Class=Class) %>%
        unique()
    )) %>%
      mutate(Class=case_when(Class %in% c("Ephemeral","E")~"E",
                             Class %in% c("Intermittent","I")~"I",
                             Class %in% c("Perennial","P")~"P",
                             Class %in% c("Unknown","U","")~"U",
                             is.na(Class)~"U",
                             T~"U"
      )) %>%
      unique() %>%
      arrange(SiteCode) %>%
      group_by(SiteCode) %>%
      mutate(n=length(SiteCode)) %>%
      ungroup() %>%
      
      mutate(Region_detail=case_when(str_detect(SiteCode,"AW")~"AW",
                                     str_detect(SiteCode,"WM")~"WM",
                                     str_detect(SiteCode,"CB")~"GP_C",
                                     str_detect(SiteCode,"CV")~"GP_C",
                                     str_detect(SiteCode,"UB")~"GP_U",
                                     str_detect(SiteCode,"UV")~"GP_U",
                                     str_detect(SiteCode,"NB")~"GP_N",
                                     str_detect(SiteCode,"NV")~"GP_N",
                                     str_detect(SiteCode,"SB")~"GP_S",
                                     str_detect(SiteCode,"SV")~"GP_S",
                                     str_detect(SiteCode,"PR")~"CB",
                                     str_detect(SiteCode,"VI")~"CB",
                                     str_detect(SiteCode,"NE")~"NE",
                                     str_detect(SiteCode,"SE")~"SE",
                                     T~"Other"),
             Region=case_when(Region_detail %in% c("AW")~"West",
                              Region_detail %in% c("WM")~"West",
                              Region_detail %in% c("GP_C","GP_U","GP_N","GP_S")~"GP",
                              Region_detail %in% c("NE","SE")~"East",
                              Region_detail %in% c("CB")~"CB",
                              T~"Other")
      ) %>%
      unique() %>%
  arrange(SiteCode)


combined_xwalk %>%write.table(file="clipboard-1000",sep="\t", row.names = F)

combined_xwalk %>%
  group_by(Region,Class) %>%  tally()
group_by(SiteCode) %>% tally() %>% filter(n>1)

main_df %>%
  filter(SiteCode=="NMSB3") %>%
  select(Database, ParentGlobalID, CollectionDate)
