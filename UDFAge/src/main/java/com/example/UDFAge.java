package com.example;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class UDFAge extends UDF {

    public int evaluate(String idCard) throws ParseException {
        int age = 0;
        String birthdayStr = idCard.substring(6, 14);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date birthday = sdf.parse(birthdayStr);
        Calendar birthdayCalendar = Calendar.getInstance();
        birthdayCalendar.setTime(birthday);

        Calendar today = Calendar.getInstance();

        age = today.get(Calendar.YEAR) - birthdayCalendar.get(Calendar.YEAR);

        if (today.get(Calendar.MONTH) < birthdayCalendar.get(Calendar.MONTH)
                || (today.get(Calendar.MONTH) == birthdayCalendar.get(Calendar.MONTH)
                && today.get(Calendar.DAY_OF_MONTH) < birthdayCalendar.get(Calendar.DAY_OF_MONTH))) {
            age--;
        }

        return age;
    }
}
