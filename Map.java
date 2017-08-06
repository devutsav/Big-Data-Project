import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
public class Map extends Mapper<LongWritable,Text,Text,DoubleWritable> {
private Text keyout=new Text();
private DoubleWritable valout=new DoubleWritable();
int age,income,gender,predicted_respose,c=0;
private double mean_age1=50.42136850053376; //these were calculated in a separate program
private double mean_age0=50.24113338936274;
private double sd_age1=16.62834613068379;
private double sd_age0=16.603135314069164;
private double mean_inc1=50842.083083812846;
private double mean_inc0=50871.74647616761;
private double sd_inc0=18675.57296728772;
private double sd_inc1=18635.84367899919;
private double prob_count1=0.479775645;
private double prob_count0=0.520224354;
private double male_1=0.497174994;
private double female_1=0.502825005;
private double male_0=0.496806339;
private double female_0=0.50319366;
public double age_prob1(int age)
{
  double z=(age-mean_age1)/sd_age1;
  double expo = -(((z-mean_age1)*(z-mean_age1))/(2*sd_age1*sd_age1)); 
  double pdf = (double)(1/(2.506627216*sd_age1))*(Math.exp(expo));
  return pdf;
}
public double income_prob1(int income)
{
  double z=(income-mean_inc1)/sd_inc1;
  double expo = -(((z-mean_inc1)*(z-mean_inc1))/(2*sd_inc1*sd_inc1));
  double pdf = (double)(1/(2.506627216*sd_inc1))*(Math.exp(expo));
  return pdf;
}
public double age_prob0(int age)
{
  double z=(age-mean_age0)/sd_age0;double expo = -(((z-mean_age0)*(z-mean_age0))/(2*sd_age0*sd_age0));
  double pdf = (double)(1/(2.506627216*sd_age0))*(Math.exp(expo));
  return pdf;
}
public double income_prob0(int income)
{
  double z=((income-mean_inc0)/sd_inc0);
  double expo = -(((z-mean_inc0)*(z-mean_inc0))/(2*sd_inc0*sd_inc0));
  double pdf = (double)((1/(2.506627216*sd_inc0))*(Math.exp(expo)));
  return pdf;
}
public double prob1(int age,int income,int gender)
{
  if(gender==1)
    return age_prob1(age)*income_prob1(income)*prob_count1;
  else
    return age_prob1(age)*income_prob1(income)*prob_count1;
}
public double prob0(int age,int income,int gender)
{
  if(gender==1)
    return age_prob0(age)*income_prob0(income)*prob_count0;
  else
    return age_prob0(age)*income_prob0(income)*prob_count0;
}
@Override
protected void map(LongWritable key, Text value,Context context)throws IOException,
InterruptedException {
// TODO Auto-generated method stub
String record=value.toString().trim();
String[] fields=record.split(",");
age=Integer.parseInt(fields[0]);
income=Integer.parseInt(fields[1]);
gender=Integer.parseInt(fields[2]);
double probability1=prob1(age,income,gender);
double probability0=prob0(age,income,gender);
//System.out.println("probability1");
//System.out.println("probability0");
if(probability1>probability0)
{
  predicted_respose=1;
}
else if(probability1<probability0){
  predicted_respose=0;
}
keyout.set(record);
valout.set(predicted_respose);
context.write(keyout, valout);
  }
}
