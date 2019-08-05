# DataUSA Election Data Quirks
The two sets of state and county data are sourced from different places, which explains most of the “small” differences between the two data sets.

### County data:
The county data are based on Charles Stewart's personal data gathering. The data is sourced from the official certified returns as published by the states and made available either online or on paper (for earlier years).   However, these two sources often disagree. The key way these two sets of data differ is that the sum of a state's county totals do not match the state data totals.

### State data:
The state returns come from the Clerk of the U.S. House of Representatives, and are supposed to be based on the returns reported by the states to the House of Representatives.

### Other known issues:
Alaska in 2004: Alaska does not have counties, but instead reports election returns by state house of representatives district. Those reports exist in 40 separate pdf files. If you go through each of those documents and record the total number of votes for each candidate and then sum them up, you get a total that is about 50% greater than the statewide vote total reported by the state. The state version is correct, while the county is not.

Because Alaska does not report by counties or their version of counties (burrows), the shapefile for counties does not line up with the county data for Alaska. This is because Alaska reports by "House District", which only exists in Alaska. Each house district has been given a unique FIPS id that follows 99999AK027YY where YY is the number of house district from 01-20.
