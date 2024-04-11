import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

@Description(name = "calculate_age",
        value = "_FUNC_(idCard) - Calculate age from ID card",
        extended = "Example:\n"
                + "  SELECT calculate_age(idCard) FROM table_name"
)
public class UDFAge extends UDF {

    public int evaluate(String idCard) throws ParseException {
        int age = 0;
        String birthdayStr = idCard.substring(6, 14); // 身份证号码中生日的字符串表示
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date birthday = sdf.parse(birthdayStr);
        Calendar birthdayCalendar = Calendar.getInstance();
        birthdayCalendar.setTime(birthday);

        Calendar today = Calendar.getInstance();

        age = today.get(Calendar.YEAR) - birthdayCalendar.get(Calendar.YEAR);

        // 如果今年的生日还没过，则年龄减一
        if (today.get(Calendar.MONTH) < birthdayCalendar.get(Calendar.MONTH)
                || (today.get(Calendar.MONTH) == birthdayCalendar.get(Calendar.MONTH)
                && today.get(Calendar.DAY_OF_MONTH) < birthdayCalendar.get(Calendar.DAY_OF_MONTH))) {
            age--;
        }

        return age;
    }
}
