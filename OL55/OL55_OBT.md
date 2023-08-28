n the past, we tried to test your query to PDT and join them to “Booking Container” explore, but don’t consider it as a way to reduce performance. The number of join paths only reduces by 1 or 2 when applying PDT into the explore

The main points of the new solution are:

- The explore is currently consisting of 2 fact tables (dms_bkg_master and dms_bkg_cntr), with several dim tables
- Instead of declaring several join path, we will create PDT of 2 big tables, which includes each fact tables and all dim tables included in the join path
- As a result, dashboard used the explore does not need to query with several join condition, and extract field from 2 big tables only
- Additionally, Booking Container explore will include only 2 PDT views, which reduces maintainence effort.

The steps to carried out above solution will take up a bit of development effort from Joon team, which can increase the hour quite a bit. Therefore, I think it would be great if we can have a meeting to discuss and align expectation between Joon and One team before implementing this.