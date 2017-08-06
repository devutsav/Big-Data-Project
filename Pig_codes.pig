//Pig Code: (On Training set, for calculation of mean and standard deviation)

offer5 = load 'traindata' as (age:float,income:float,gender:int,offer:int,response:int);
offer5_1 = filter offer5 by response == 1;
offer5_0 = filter offer5 by response == 0;
grp_1 = group offer5_1 ALL;
grp_0 = group offer5_0 ALL;
avg_1 = foreach grp_1 generate AVG(offer5_1.age) as avgage1 , AVG(offer5_1.income) as avginc1 ,
COUNT(offer5_1) as c1;
avg_0 = foreach grp_0 generate AVG(offer5_0.age) as avgage0 , AVG(offer5_0.income) as avginc0 ,
COUNT(offer5_0) as c0;
std_1 = foreach offer5_1 generate (age-50.42136850053376)*(age-50.42136850053376) as sdage_1 ,
(income - 50842.083083812846)*(income - 50842.083083812846) as sdincome_1;
std_0 = foreach offer5_0 generate (age-50.24113338936274)*(age-50.24113338936274) as sdage_0 ,
(income - 50871.74647616761)*(income - 50871.74647616761) as sdincome_0;
grp_std_1 = group std_1 ALL;grp_std_0 = group std_0 ALL;
finalstd_1 = foreach grp_std_1 generate SQRT(AVG(std_1.sdage_1)) as stdage1 ,
SQRT(AVG(std_1.sdincome_1)) as stdinc1 ;
finalstd_0 = foreach grp_std_0 generate SQRT(AVG(std_0.sdage_0)) as stdage0 ,
SQRT(AVG(std_0.sdincome_0)) as stdinc0 ;
gender_m_1 = filter offer5 by gen == 1 and res == 1;
total_res1 = filter offer5 by res == 1;
dump total_res1;
