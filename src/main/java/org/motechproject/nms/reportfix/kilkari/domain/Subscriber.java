package org.motechproject.nms.reportfix.kilkari.domain;

import java.sql.Timestamp;
import java.util.Date;

/**
 * Representation of a subscriber from the reporting perspective *
 * ps. case here matches the reporting subscriber table casing
 */
public class Subscriber {

    private String Name;

    private String Language;

    private int Age_Of_Beneficiary;

    private Date Date_Of_Birth;

    private int State_ID;

    private int District_ID;

    private int Taluka_ID;

    private int Village_ID;

    private int HBlock_ID;

    private int HFacility_ID;

    private int HSub_Facility_ID;

    private Timestamp Last_Modified_Time;

    private Date Lmp;

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public String getLanguage() {
        return Language;
    }

    public void setLanguage(String language) {
        Language = language;
    }

    public int getAge_Of_Beneficiary() {
        return Age_Of_Beneficiary;
    }

    public void setAge_Of_Beneficiary(int age_Of_Beneficiary) {
        Age_Of_Beneficiary = age_Of_Beneficiary;
    }

    public Date getDate_Of_Birth() {
        return Date_Of_Birth;
    }

    public void setDate_Of_Birth(Date date_Of_Birth) {
        Date_Of_Birth = date_Of_Birth;
    }

    public int getState_ID() {
        return State_ID;
    }

    public void setState_ID(int state_ID) {
        State_ID = state_ID;
    }

    public int getDistrict_ID() {
        return District_ID;
    }

    public void setDistrict_ID(int district_ID) {
        District_ID = district_ID;
    }

    public int getTaluka_ID() {
        return Taluka_ID;
    }

    public void setTaluka_ID(int taluka_ID) {
        Taluka_ID = taluka_ID;
    }

    public int getVillage_ID() {
        return Village_ID;
    }

    public void setVillage_ID(int village_ID) {
        Village_ID = village_ID;
    }

    public int getHBlock_ID() {
        return HBlock_ID;
    }

    public void setHBlock_ID(int HBlock_ID) {
        this.HBlock_ID = HBlock_ID;
    }

    public int getHFacility_ID() {
        return HFacility_ID;
    }

    public void setHFacility_ID(int HFacility_ID) {
        this.HFacility_ID = HFacility_ID;
    }

    public int getHSub_Facility_ID() {
        return HSub_Facility_ID;
    }

    public void setHSub_Facility_ID(int HSub_Facility_ID) {
        this.HSub_Facility_ID = HSub_Facility_ID;
    }

    public Timestamp getLast_Modified_Time() {
        return Last_Modified_Time;
    }

    public void setLast_Modified_Time(Timestamp last_Modified_Time) {
        Last_Modified_Time = last_Modified_Time;
    }

    public Date getLmp() {
        return Lmp;
    }

    public void setLmp(Date lmp) {
        Lmp = lmp;
    }
}
