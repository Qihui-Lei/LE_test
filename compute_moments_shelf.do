/*==============================================================================
  PSID-SHELF Moments Computation
  ===============================
  Computes structural model moments and reduced-form analyses using 
  PSID-SHELF (long format).
  
  Blocks computable with SHELF alone:
    Block B (partial): Earnings event study + E-to-U transitions (45 of 50)
    Block C (partial): High-amenity occ share by kid age × tercile (9 of 21)
    Block D (full):    Fertility & initial conditions (18 moments)
    Block E (full):    Position-dependent recovery (6 moments)
    Fact 3:            State PFL DiD (earnings + employment)
    Fact 6:            Occupation-amenity sorting (revealed preference)
    Extra:             Recovery-dynamics by tercile, Kleven event study,
                       mother/non-mother gap, lifecycle profiles
  
  NOT computable (need supplemental data):
    Block A:  GKOS SSA administrative data
    Block B:  Hourly wage change (no hours/wage rate in SHELF)
    Block C:  Annual hours moments (no hours in SHELF pre-2017)
    Block F:  GKOS SSA arc-percent moments
    Fact 2:   FMLA threshold (no employer size in SHELF)
    Fact 5:   Hours vs. wage-rate decomposition (no hours/wage rate)
  
  Input:  PSID-SHELF long-format .dta (psidshelf_long.dta)
  Output: moments_psid_shelf.csv
  
  Assumes PSID-SHELF variable names from Data Release 2025-01.
==============================================================================*/

clear all
set more off
set matsize 5000

*--- USER: set paths here ---
global datadir "."
global outdir  "."

capture log close
log using "${outdir}/moments_psid_shelf.log", replace text

/*==============================================================================
  SECTION 0: Load data
==============================================================================*/

use "${datadir}/psidshelf_long.dta", clear

di "Observations loaded: " _N
di "Variables: " c(k)

*--- Create a tempfile to store all moments ---
tempfile moments_file
postfile moments_handle str60 block str80 moment_name double value ///
    double se int N str20 unit using `moments_file', replace

/*==============================================================================
  SECTION 1: Sample construction — Women
==============================================================================*/

*--- Keep women only ---
keep if DEMO_SEX == 2

*--- Keep sample persons (exclude nonsample) ---
keep if SAMPSTAT >= 1 & SAMPSTAT <= 4

*--- Keep observations where person is active (current FU member) ---
keep if PANEL_CURRENT == 1

*--- Generate age from birth year ---
gen age = YEAR - DEMO_BIRTH_YEAR
label var age "Age (survey year - birth year)"

*--- Real earnings in 2024 USD (tax year = survey year - 1 for biennial) ---
* EARN_TOT_RD is already inflation-adjusted
* Note: earnings refer to the prior tax year
gen earn_real = EARN_TOT_RD
replace earn_real = 0 if earn_real == . | earn_real < 0
label var earn_real "Individual real labor earnings (2024 USD)"

*--- Positive earnings indicator ---
gen has_earn = (earn_real > 0 & earn_real < .)
label var has_earn "Has positive earnings"

*--- Employment indicator ---
gen employed = (EMP_WORK == 1) if EMP_WORK < .
label var employed "Currently working (from EMP_WORK)"

*--- Education: college indicator ---
* EDU_LEVEL_MAX: 1=<HS, 2=HS, 3=some college, 4=BA, 5=postgrad
gen college = (EDU_LEVEL_MAX >= 4) if EDU_LEVEL_MAX < .
label var college "Has bachelor's degree or higher"

di "Female sample-person observations: " _N

/*==============================================================================
  SECTION 2: First-birth identification from child records
==============================================================================*/

*--- Find first biological child's birth year ---
* REL_CHI{k}_BYEAR: birth year of child k
* REL_CHI{k}_TYPE:  1 = biological, 2 = adopted (per SHELF codebook)

* Take minimum biological child birth year across up to 20 child slots
gen first_bio_birth_year = .
forvalues k = 1/20 {
    capture confirm variable REL_CHI`k'_BYEAR
    if _rc == 0 {
        capture confirm variable REL_CHI`k'_TYPE
        if _rc == 0 {
            * Biological child (TYPE == 1)
            replace first_bio_birth_year = ///
                min(first_bio_birth_year, REL_CHI`k'_BYEAR) ///
                if REL_CHI`k'_TYPE == 1 & REL_CHI`k'_BYEAR > 1900 ///
                & REL_CHI`k'_BYEAR < .
        }
        else {
            * If TYPE variable missing, use birth year directly
            replace first_bio_birth_year = ///
                min(first_bio_birth_year, REL_CHI`k'_BYEAR) ///
                if REL_CHI`k'_BYEAR > 1900 & REL_CHI`k'_BYEAR < .
        }
    }
}

*--- Age at first birth ---
gen age_first_birth = first_bio_birth_year - DEMO_BIRTH_YEAR
label var age_first_birth "Age at first biological birth"

*--- Has any children ---
gen has_children = (REL_CHI_TOT > 0 & REL_CHI_TOT < .) 
label var has_children "Ever had children (from child records)"

*--- Total biological children ---
gen n_bio_children = 0
forvalues k = 1/20 {
    capture confirm variable REL_CHI`k'_TYPE
    if _rc == 0 {
        replace n_bio_children = n_bio_children + 1 ///
            if REL_CHI`k'_TYPE == 1 & REL_CHI`k'_BYEAR > 1900 ///
            & REL_CHI`k'_BYEAR < .
    }
}
label var n_bio_children "Number of biological children"

*--- Event time relative to first birth ---
* Income in PSID is for the prior tax year, so event_time for earnings
* is approximately YEAR - first_bio_birth_year
* (e.g., 2005 wave reports 2004 income; if birth in 2004, event_time = 1)
gen event_time = YEAR - first_bio_birth_year if first_bio_birth_year < .
label var event_time "Event time: survey year - first birth year"

*--- First birth at age 25-35 (sample restriction for event studies) ---
gen birth_25_35 = (age_first_birth >= 25 & age_first_birth <= 35) ///
    if age_first_birth < .
label var birth_25_35 "First birth between ages 25-35"

*--- Report sample sizes ---
di "=== SAMPLE SUMMARY ==="
tab has_children if age >= 25 & age <= 55, m
sum age_first_birth if birth_25_35 == 1, d
di "Women with first birth at 25-35: " r(N)

/*==============================================================================
  SECTION 3: Pre-birth earnings quintiles and terciles
==============================================================================*/

*--- Compute average pre-birth earnings (3 years before birth) ---
* For each woman, average EARN_TOT_RD when event_time in {-3, -2, -1}
* Require >=2 years of positive earnings

preserve
    keep if event_time >= -3 & event_time <= -1 & birth_25_35 == 1
    
    * Count years with positive earnings
    bysort ID: egen n_pos_prebirth = total(has_earn)
    
    * Mean earnings in pre-birth window
    bysort ID: egen avg_prebirth_earn = mean(earn_real) if has_earn == 1
    
    * Fill within person
    bysort ID: egen avg_prebirth = max(avg_prebirth_earn)
    
    * Keep one obs per person
    bysort ID: keep if _n == 1
    
    * Require >=2 positive-earnings years
    keep if n_pos_prebirth >= 2
    
    * Quintiles and terciles (within age-at-birth bins for age-specificity)
    * Simple version: unconditional quintiles
    xtile prebirth_quintile = avg_prebirth, nq(5)
    xtile prebirth_tercile  = avg_prebirth, nq(3)
    
    keep ID avg_prebirth prebirth_quintile prebirth_tercile n_pos_prebirth
    tempfile prebirth_ranks
    save `prebirth_ranks'
restore

*--- Merge back ---
merge m:1 ID using `prebirth_ranks', nogen keep(master match)

label var prebirth_quintile "Pre-birth earnings quintile (1=lowest)"
label var prebirth_tercile  "Pre-birth earnings tercile (1=lowest)"

di "Women with pre-birth quintile assigned: "
count if prebirth_quintile < .

/*==============================================================================
  SECTION 4: BLOCK B — Birth-Event Dynamics
==============================================================================*/

di ""
di "=========================================="
di "  BLOCK B: Birth-Event Dynamics"
di "=========================================="

*--- B.1: Earnings path around 1st birth × 5 quintiles × 8 event times ---
* Event times: -3, -2, -1, 0, 1, 2, 3, 5
* (biennial gap means we use available observations)

preserve
    keep if birth_25_35 == 1 & prebirth_quintile < .
    keep if inlist(event_time, -3, -2, -1, 0, 1, 2, 3, 5)
    keep if age >= 22 & age <= 55
    
    * Normalize: earnings relative to pre-birth average
    gen earn_ratio = earn_real / avg_prebirth if avg_prebirth > 0
    
    * Compute mean normalized earnings by quintile × event time
    collapse (mean) earn_ratio (sd) sd_ratio=earn_ratio ///
        (count) N=earn_ratio, by(prebirth_quintile event_time)
    
    gen se = sd_ratio / sqrt(N)
    
    * Post moments
    forvalues q = 1/5 {
        forvalues et = -3/5 {
            if `et' == 4 continue
            sum earn_ratio if prebirth_quintile == `q' & event_time == `et', meanonly
            if r(N) > 0 {
                local mn = r(mean)
                sum se if prebirth_quintile == `q' & event_time == `et', meanonly
                local s = r(mean)
                sum N if prebirth_quintile == `q' & event_time == `et', meanonly
                local n = r(mean)
                post moments_handle ("B") ///
                    ("B1_earn_path_q`q'_t`et'") (`mn') (`s') (`n') ("ratio")
            }
        }
    }
restore

*--- B.2: E-to-U transition rate at birth × 5 quintiles ---
* Define: employed in year before birth (event_time == -1 or 0) 
* but not employed at birth year (event_time == 0 or 1)

preserve
    keep if birth_25_35 == 1 & prebirth_quintile < .
    
    * Employed before birth
    gen emp_pre = employed if event_time == -1
    gen emp_post = employed if event_time == 1
    
    * Get pre and post employment for each person
    bysort ID: egen emp_before = max(emp_pre)
    bysort ID: egen emp_after  = max(emp_post)
    
    * E-to-U: employed before, not employed after
    gen etu = (emp_before == 1 & emp_after == 0) if emp_before < . & emp_after < .
    
    bysort ID: keep if _n == 1
    keep if etu < .
    
    forvalues q = 1/5 {
        sum etu if prebirth_quintile == `q', meanonly
        if r(N) > 0 {
            local mn = r(mean)
            local n = r(N)
            local s = sqrt(`mn' * (1-`mn') / `n')
            post moments_handle ("B") ///
                ("B2_etu_rate_q`q'") (`mn') (`s') (`n') ("rate")
        }
    }
restore

/*==============================================================================
  SECTION 5: BLOCK D — Fertility and Initial Conditions
==============================================================================*/

di ""
di "=========================================="
di "  BLOCK D: Fertility & Initial Conditions"
di "=========================================="

*--- D.1: Age at first birth × 5 LE quintiles ---
* Approximate LE quintiles using average earnings 25-45 
* (or pre-birth quintile as proxy)

* For broader sample: compute average earnings over observed ages 25-45
preserve
    keep if age >= 25 & age <= 45 & has_earn == 1
    bysort ID: egen avg_earn_2545 = mean(earn_real)
    bysort ID: egen n_yrs_2545 = count(earn_real)
    bysort ID: keep if _n == 1
    
    keep if n_yrs_2545 >= 5    // require at least 5 years observed
    xtile le_quintile = avg_earn_2545, nq(5)
    
    keep ID le_quintile avg_earn_2545
    tempfile le_ranks
    save `le_ranks'
restore

merge m:1 ID using `le_ranks', nogen keep(master match)
label var le_quintile "LE proxy quintile (avg earnings 25-45)"

* Age at first birth by LE quintile
preserve
    bysort ID: keep if _n == 1
    keep if age_first_birth >= 15 & age_first_birth <= 45
    keep if le_quintile < .
    
    forvalues q = 1/5 {
        sum age_first_birth if le_quintile == `q', meanonly
        if r(N) > 0 {
            post moments_handle ("D") ///
                ("D1_age_first_birth_q`q'") (r(mean)) (.) (r(N)) ("years")
        }
    }
restore

*--- D.2: Total children × 5 LE quintiles ---
preserve
    bysort ID: keep if _n == 1
    keep if le_quintile < .
    keep if age >= 40  // completed fertility
    
    forvalues q = 1/5 {
        sum n_bio_children if le_quintile == `q', meanonly
        if r(N) > 0 {
            post moments_handle ("D") ///
                ("D2_total_children_q`q'") (r(mean)) (.) (r(N)) ("count")
        }
    }
restore

*--- D.3: Fraction with children at 25 × education ---
preserve
    keep if age == 25
    bysort ID: keep if _n == 1
    
    * Has child by age 25: first_bio_birth_year <= DEMO_BIRTH_YEAR + 25
    gen has_child_at25 = (first_bio_birth_year <= DEMO_BIRTH_YEAR + 25) ///
        if first_bio_birth_year < .
    replace has_child_at25 = 0 if has_child_at25 == .
    
    foreach edlev in 0 1 {
        local edlab = cond(`edlev'==1, "college", "noncollege")
        sum has_child_at25 if college == `edlev', meanonly
        if r(N) > 0 {
            local mn = r(mean)
            local n  = r(N)
            local s  = sqrt(`mn'*(1-`mn')/`n')
            post moments_handle ("D") ///
                ("D3_frac_child_at25_`edlab'") (`mn') (`s') (`n') ("share")
        }
    }
restore

*--- D.4: Employment rate at 25 × education ---
preserve
    keep if age == 25 & employed < .
    bysort ID: keep if _n == 1
    
    foreach edlev in 0 1 {
        local edlab = cond(`edlev'==1, "college", "noncollege")
        sum employed if college == `edlev', meanonly
        if r(N) > 0 {
            local mn = r(mean)
            local n  = r(N)
            local s  = sqrt(`mn'*(1-`mn')/`n')
            post moments_handle ("D") ///
                ("D4_emprate_at25_`edlab'") (`mn') (`s') (`n') ("rate")
        }
    }
restore

*--- D.5: Mean earnings at 25 × education ---
preserve
    keep if age == 25 & has_earn == 1
    bysort ID: keep if _n == 1
    
    foreach edlev in 0 1 {
        local edlab = cond(`edlev'==1, "college", "noncollege")
        sum earn_real if college == `edlev'
        if r(N) > 0 {
            post moments_handle ("D") ///
                ("D5_mean_earn_at25_`edlab'") (r(mean)) (r(sd)/sqrt(r(N))) ///
                (r(N)) ("USD2024")
        }
    }
restore

*--- D.6: Earnings growth 25-35 among early mothers × education ---
* Early mothers: first birth before age 25
preserve
    keep if age_first_birth < 25 & age_first_birth >= 15
    keep if inlist(age, 25, 35)
    keep if has_earn == 1
    
    * Reshape to get earn at 25 and 35
    gen earn_25 = earn_real if age == 25
    gen earn_35 = earn_real if age == 35
    
    bysort ID: egen e25 = max(earn_25)
    bysort ID: egen e35 = max(earn_35)
    bysort ID: keep if _n == 1
    
    keep if e25 > 0 & e25 < . & e35 > 0 & e35 < .
    gen log_growth = ln(e35) - ln(e25)
    
    foreach edlev in 0 1 {
        local edlab = cond(`edlev'==1, "college", "noncollege")
        sum log_growth if college == `edlev'
        if r(N) > 0 {
            post moments_handle ("D") ///
                ("D6_earngrowth_2535_earlymom_`edlab'") ///
                (r(mean)) (r(sd)/sqrt(r(N))) (r(N)) ("log_diff")
        }
    }
restore

/*==============================================================================
  SECTION 6: BLOCK E — Position-Dependent Recovery
==============================================================================*/

di ""
di "=========================================="
di "  BLOCK E: Position-Dependent Recovery"
di "=========================================="

*--- Earnings at t+3 and t+5 post-birth relative to pre-birth × 3 terciles ---

preserve
    keep if birth_25_35 == 1 & prebirth_tercile < .
    keep if avg_prebirth > 0
    
    gen earn_ratio = earn_real / avg_prebirth
    
    * Keep event times 3 and 5
    keep if inlist(event_time, 3, 5)
    
    collapse (mean) earn_ratio (sd) sd_ratio=earn_ratio ///
        (count) N=earn_ratio, by(prebirth_tercile event_time)
    
    gen se = sd_ratio / sqrt(N)
    
    forvalues t = 1/3 {
        foreach et in 3 5 {
            sum earn_ratio if prebirth_tercile == `t' & event_time == `et', meanonly
            if r(N) > 0 {
                local mn = r(mean)
                sum se if prebirth_tercile == `t' & event_time == `et', meanonly
                local s = r(mean)
                sum N if prebirth_tercile == `t' & event_time == `et', meanonly
                local n = r(mean)
                post moments_handle ("E") ///
                    ("E1_recovery_tercile`t'_t`et'") (`mn') (`s') (`n') ("ratio")
            }
        }
    }
restore

/*==============================================================================
  SECTION 7: EXTRA — Long-Horizon Recovery Dynamics
  "Top-tercile women recover within 3-5 years; 
   bottom-tercile do not recover even by age 45"
==============================================================================*/

di ""
di "=========================================="
di "  EXTRA: Long-Horizon Recovery by Tercile"
di "=========================================="

preserve
    keep if birth_25_35 == 1 & prebirth_tercile < .
    keep if avg_prebirth > 0
    keep if age >= 22 & age <= 50
    
    gen earn_ratio = earn_real / avg_prebirth
    
    * Compute mean earn_ratio at each event time by tercile
    * for event times -3 to 20 (capturing long-run recovery)
    keep if event_time >= -3 & event_time <= 20
    
    collapse (mean) earn_ratio (sd) sd_ratio=earn_ratio ///
        (count) N=earn_ratio, by(prebirth_tercile event_time)
    
    gen se = sd_ratio / sqrt(N)
    
    * Post all cells as supplementary moments
    forvalues t = 1/3 {
        levelsof event_time if prebirth_tercile == `t', local(ets)
        foreach et of local ets {
            sum earn_ratio if prebirth_tercile == `t' & event_time == `et', meanonly
            if r(N) >= 20 {
                local mn = r(mean)
                sum se if prebirth_tercile == `t' & event_time == `et', meanonly
                local s = r(mean)
                sum N if prebirth_tercile == `t' & event_time == `et', meanonly
                local n = r(mean)
                post moments_handle ("EXTRA_recovery") ///
                    ("recovery_tercile`t'_t`et'") (`mn') (`s') (`n') ("ratio")
            }
        }
    }
restore

*--- Recovery at age 45 for each tercile ---
preserve
    keep if birth_25_35 == 1 & prebirth_tercile < .
    keep if avg_prebirth > 0
    keep if age == 45
    
    gen earn_ratio_45 = earn_real / avg_prebirth
    
    forvalues t = 1/3 {
        sum earn_ratio_45 if prebirth_tercile == `t'
        if r(N) > 0 {
            post moments_handle ("EXTRA_recovery") ///
                ("recovery_at_age45_tercile`t'") ///
                (r(mean)) (r(sd)/sqrt(r(N))) (r(N)) ("ratio")
        }
    }
restore

/*==============================================================================
  SECTION 8: BLOCK B supplementary — Earnings event study (pooled Kleven)
==============================================================================*/

di ""
di "=========================================="
di "  BLOCK B (supplementary): Kleven Event Study (pooled)"
di "=========================================="

*--- Pooled child penalty (not by quintile) ---
* Estimate: Y_it = Σ_τ α_τ D^event_τ + β age_FE + γ year_FE + ε

preserve
    keep if birth_25_35 == 1
    keep if event_time >= -5 & event_time <= 10
    keep if age >= 20 & age <= 55
    
    * Log earnings (with floor)
    gen log_earn = ln(max(earn_real, 1))
    
    * Event-time dummies (omit τ = -1)
    tab event_time, gen(evt_)
    
    * Find which dummy corresponds to event_time == -1
    * and use it as reference
    qui sum event_time
    local min_et = r(min)
    local ref_col = -1 - `min_et' + 1
    
    * Simple OLS with age and year FE
    capture reghdfe log_earn ibn.event_time i.age, ///
        absorb(YEAR) nocons vce(cluster ID)
    
    if _rc == 0 {
        * Extract event-time coefficients
        forvalues et = -5/10 {
            capture {
                local b = _b[`et'.event_time]
                local s = _se[`et'.event_time]
                post moments_handle ("B_kleven") ///
                    ("kleven_pooled_t`et'") (`b') (`s') (e(N)) ("log_earn")
            }
        }
    }
    else {
        * Fallback: simple mean log earnings by event time
        collapse (mean) log_earn (sd) sd_le=log_earn ///
            (count) N=log_earn, by(event_time)
        
        * Normalize to event_time == -1
        sum log_earn if event_time == -1, meanonly
        local base = r(mean)
        gen penalty = log_earn - `base'
        gen se = sd_le / sqrt(N)
        
        levelsof event_time, local(ets)
        foreach et of local ets {
            sum penalty if event_time == `et', meanonly
            local mn = r(mean)
            sum se if event_time == `et', meanonly
            local s = r(mean)
            sum N if event_time == `et', meanonly
            local n = r(mean)
            post moments_handle ("B_kleven") ///
                ("kleven_pooled_t`et'") (`mn') (`s') (`n') ("log_diff")
        }
    }
restore

/*==============================================================================
  SECTION 9: FACT 3 — State Paid Family Leave DiD
  PFL adoption: CA=2004, NJ=2009, RI=2014, NY=2018
  PSID state codes: CA=4, NJ=29, RI=38, NY=31
==============================================================================*/

di ""
di "=========================================="
di "  FACT 3: State Paid Family Leave DiD"
di "=========================================="

*--- Map PSID state codes to PFL status ---
gen pfl_state = 0
gen pfl_year  = .
replace pfl_state = 1 if GEO_STATE == 4   // California
replace pfl_year  = 2004 if GEO_STATE == 4
replace pfl_state = 1 if GEO_STATE == 29  // New Jersey
replace pfl_year  = 2009 if GEO_STATE == 29
replace pfl_state = 1 if GEO_STATE == 38  // Rhode Island
replace pfl_year  = 2014 if GEO_STATE == 38
replace pfl_state = 1 if GEO_STATE == 31  // New York
replace pfl_year  = 2018 if GEO_STATE == 31

label var pfl_state "Lives in a state that ever adopted PFL"
label var pfl_year  "Year PFL took effect in state"

*--- Post-PFL indicator ---
gen post_pfl = (YEAR >= pfl_year) if pfl_state == 1
replace post_pfl = 0 if pfl_state == 0
label var post_pfl "State PFL is in effect (state adopted & year >= adoption)"

*--- Is a recent mother (first birth within 3 years of survey) ---
gen recent_mother = (event_time >= 0 & event_time <= 3) if event_time < .
replace recent_mother = 0 if recent_mother == .
label var recent_mother "Had first birth within 3 years"

*--- 9a. Simple DiD: earnings of childbearing-age women in PFL vs non-PFL ---
* Sample: women 25-45 in states with valid state codes
preserve
    keep if age >= 25 & age <= 45
    keep if GEO_STATE >= 1 & GEO_STATE <= 51
    keep if earn_real < .
    
    * Earnings outcome
    gen log_earn = ln(max(earn_real, 1))
    
    * DiD for recent mothers: PFL × post
    * Use only PFL states + never-PFL states (exclude WA/OR/CO which
    * adopted after SHELF's 2021 endpoint or right at it)
    
    * Window: 5 years before and after adoption (for each PFL state)
    gen in_pfl_window = 0
    replace in_pfl_window = 1 if pfl_state == 1 ///
        & YEAR >= pfl_year - 5 & YEAR <= pfl_year + 5
    replace in_pfl_window = 1 if pfl_state == 0  // controls always in
    
    keep if in_pfl_window == 1
    
    * Simple 2x2 DiD: mean earnings by PFL × post
    * Among recent mothers
    forvalues pfl = 0/1 {
        forvalues post = 0/1 {
            local pfl_lab  = cond(`pfl'==1, "pfl", "nopfl")
            local post_lab = cond(`post'==1, "post", "pre")
            
            sum earn_real if pfl_state == `pfl' & post_pfl == `post' ///
                & recent_mother == 1
            if r(N) >= 20 {
                post moments_handle ("F3_PFL") ///
                    ("F3_earn_`pfl_lab'_`post_lab'_mothers") ///
                    (r(mean)) (r(sd)/sqrt(r(N))) (r(N)) ("USD2024")
            }
            
            * Also for all childbearing-age women
            sum earn_real if pfl_state == `pfl' & post_pfl == `post'
            if r(N) >= 20 {
                post moments_handle ("F3_PFL") ///
                    ("F3_earn_`pfl_lab'_`post_lab'_allwomen") ///
                    (r(mean)) (r(sd)/sqrt(r(N))) (r(N)) ("USD2024")
            }
        }
    }
    
    * DiD estimate: (PFL_post - PFL_pre) - (noPFL_post - noPFL_pre)
    * Among recent mothers
    qui sum earn_real if pfl_state==1 & post_pfl==1 & recent_mother==1
    local y11 = r(mean)
    qui sum earn_real if pfl_state==1 & post_pfl==0 & recent_mother==1
    local y10 = r(mean)
    qui sum earn_real if pfl_state==0 & post_pfl==1 & recent_mother==1
    local y01 = r(mean)
    qui sum earn_real if pfl_state==0 & post_pfl==0 & recent_mother==1
    local y00 = r(mean)
    
    local did = (`y11' - `y10') - (`y01' - `y00')
    post moments_handle ("F3_PFL") ("F3_DiD_mothers") (`did') (.) (.) ("USD2024")
    
    di "  DiD estimate (recent mothers): " %9.0f `did'
    
    * 9b. Regression DiD with controls
    capture {
        reg log_earn i.pfl_state##i.post_pfl i.age i.YEAR ///
            i.college i.RACE_ETH_MAJ_COL ///
            if recent_mother == 1, vce(cluster FUID)
        
        local did_coef = _b[1.pfl_state#1.post_pfl]
        local did_se   = _se[1.pfl_state#1.post_pfl]
        local did_n    = e(N)
        
        post moments_handle ("F3_PFL") ///
            ("F3_DiD_reg_mothers") (`did_coef') (`did_se') (`did_n') ("log_earn")
        
        di "  Regression DiD (log earn, recent mothers): " ///
            %6.3f `did_coef' " (SE=" %6.3f `did_se' ")"
    }
    
    * 9c. Employment DiD
    capture {
        reg employed i.pfl_state##i.post_pfl i.age i.YEAR ///
            i.college i.RACE_ETH_MAJ_COL ///
            if recent_mother == 1, vce(cluster FUID)
        
        local did_coef = _b[1.pfl_state#1.post_pfl]
        local did_se   = _se[1.pfl_state#1.post_pfl]
        local did_n    = e(N)
        
        post moments_handle ("F3_PFL") ///
            ("F3_DiD_reg_emp_mothers") (`did_coef') (`did_se') (`did_n') ("pp")
        
        di "  Regression DiD (employment, recent mothers): " ///
            %6.3f `did_coef' " (SE=" %6.3f `did_se' ")"
    }
restore

* 9d. Event study around PFL adoption (CA only, largest sample)
preserve
    keep if GEO_STATE == 4 | pfl_state == 0  // CA vs non-PFL states
    keep if age >= 25 & age <= 40
    keep if recent_mother == 1
    
    * Event time relative to CA PFL (2004)
    gen pfl_event = YEAR - 2004
    keep if pfl_event >= -5 & pfl_event <= 8
    
    * Mean earnings by PFL status × event time
    collapse (mean) earn_real (count) N=earn_real, ///
        by(pfl_state pfl_event)
    
    * Post event-study cells
    forvalues pfl = 0/1 {
        local lab = cond(`pfl'==1, "CA", "control")
        levelsof pfl_event if pfl_state == `pfl', local(ets)
        foreach et of local ets {
            sum earn_real if pfl_state == `pfl' & pfl_event == `et', meanonly
            if r(N) > 0 {
                local mn = r(mean)
                sum N if pfl_state == `pfl' & pfl_event == `et', meanonly
                local n = r(mean)
                post moments_handle ("F3_PFL_ES") ///
                    ("F3_eventstudy_`lab'_t`et'") (`mn') (.) (`n') ("USD2024")
            }
        }
    }
restore


/*==============================================================================
  SECTION 10: FACT 6 — Occupation-Amenity Sorting (Revealed Preference)
  Classify occupations by mother-overrepresentation, then track sorting
  around birth by pre-birth quintile.
==============================================================================*/

di ""
di "=========================================="
di "  FACT 6: Occupation-Amenity Sorting"
di "=========================================="

*--- 10a. Build unified occupation variable ---
* SHELF has three coding systems; unify into a single variable
gen occ_code = .
gen occ_system = .    // 1=1970, 2=2000, 3=2010

* 1970 codes (1968-2001)
replace occ_code = OCC_1970C if OCC_1970C > 0 & OCC_1970C < .
replace occ_system = 1 if OCC_1970C > 0 & OCC_1970C < .

* 2000 codes (2003-2015) — use first mention
replace occ_code = OCC_2000C_1M if OCC_2000C_1M > 0 & OCC_2000C_1M < . ///
    & occ_code == .
replace occ_system = 2 if OCC_2000C_1M > 0 & OCC_2000C_1M < . ///
    & occ_system == .

* 2010 codes (2017+) — use first mention
replace occ_code = OCC_2010C_1M if OCC_2010C_1M > 0 & OCC_2010C_1M < . ///
    & occ_code == .
replace occ_system = 3 if OCC_2010C_1M > 0 & OCC_2010C_1M < . ///
    & occ_system == .

label var occ_code   "Unified occupation code"
label var occ_system "Occupation coding system (1=1970, 2=2000, 3=2010)"

*--- 10b. Broad occupation categories (roughly comparable across systems) ---
* Create ~10 broad groups from each coding system
* 1970 Census codes: 001-195 professional/technical, 201-245 managers,
*   260-285 sales, 301-395 clerical, 401-600 crafts/operatives,
*   601-715 service, 740-785 laborers, 801-824 farmers
* 2000/2010 codes: similar broad groupings at 1000s digit
* For amenity analysis, we simplify to: professional, managerial,
*   sales, clerical/admin, service, manual/production

gen occ_broad = .

* --- 1970 codes ---
if 1 {
    replace occ_broad = 1 if occ_system==1 & occ_code >= 1   & occ_code <= 195
    replace occ_broad = 2 if occ_system==1 & occ_code >= 201 & occ_code <= 245
    replace occ_broad = 3 if occ_system==1 & occ_code >= 260 & occ_code <= 285
    replace occ_broad = 4 if occ_system==1 & occ_code >= 301 & occ_code <= 395
    replace occ_broad = 5 if occ_system==1 & occ_code >= 401 & occ_code <= 600
    replace occ_broad = 6 if occ_system==1 & occ_code >= 601 & occ_code <= 715
    replace occ_broad = 5 if occ_system==1 & occ_code >= 740 & occ_code <= 824
}

* --- 2000 codes ---
if 1 {
    replace occ_broad = 2 if occ_system==2 & occ_code >= 1    & occ_code <= 430
    replace occ_broad = 1 if occ_system==2 & occ_code >= 440  & occ_code <= 950
    replace occ_broad = 6 if occ_system==2 & occ_code >= 3600 & occ_code <= 4650
    replace occ_broad = 3 if occ_system==2 & occ_code >= 4700 & occ_code <= 4960
    replace occ_broad = 4 if occ_system==2 & occ_code >= 5000 & occ_code <= 5930
    replace occ_broad = 5 if occ_system==2 & occ_code >= 6000 & occ_code <= 9750
}

* --- 2010 codes ---
if 1 {
    replace occ_broad = 2 if occ_system==3 & occ_code >= 10   & occ_code <= 430
    replace occ_broad = 1 if occ_system==3 & occ_code >= 500  & occ_code <= 950
    replace occ_broad = 6 if occ_system==3 & occ_code >= 3600 & occ_code <= 4650
    replace occ_broad = 3 if occ_system==3 & occ_code >= 4700 & occ_code <= 4965
    replace occ_broad = 4 if occ_system==3 & occ_code >= 5000 & occ_code <= 5940
    replace occ_broad = 5 if occ_system==3 & occ_code >= 6000 & occ_code <= 9750
}

label define occ_broad_lbl 1 "Professional/Technical" 2 "Management/Business" ///
    3 "Sales" 4 "Clerical/Admin" 5 "Manual/Production" 6 "Service"
label values occ_broad occ_broad_lbl
label var occ_broad "Broad occupation group"

*--- 10c. Revealed-preference amenity classification ---
* High-amenity = occupations where mothers with children 0-5 are 
* overrepresented relative to childless women of same age

* Identify mothers with young children (youngest child 0-5)
gen has_young_child = 0
forvalues k = 1/10 {
    capture confirm variable REL_CHI`k'_BYEAR
    if _rc == 0 {
        replace has_young_child = 1 ///
            if REL_CHI`k'_BYEAR >= (YEAR - 5) & REL_CHI`k'_BYEAR <= YEAR ///
            & REL_CHI`k'_BYEAR < .
    }
}
label var has_young_child "Has a child aged 0-5"

* Also identify youngest child age bin for Block C moments
gen youngest_child_age = .
forvalues k = 1/10 {
    capture confirm variable REL_CHI`k'_BYEAR
    if _rc == 0 {
        gen _cage`k' = YEAR - REL_CHI`k'_BYEAR ///
            if REL_CHI`k'_BYEAR > 1900 & REL_CHI`k'_BYEAR < .
        replace youngest_child_age = min(youngest_child_age, _cage`k') ///
            if _cage`k' >= 0 & _cage`k' < .
        drop _cage`k'
    }
}
label var youngest_child_age "Age of youngest child"

gen kid_agebin = .
replace kid_agebin = 1 if youngest_child_age >= 0 & youngest_child_age <= 2
replace kid_agebin = 2 if youngest_child_age >= 3 & youngest_child_age <= 5
replace kid_agebin = 3 if youngest_child_age >= 6 & youngest_child_age < .
label define kid_agebin_lbl 1 "0-2" 2 "3-5" 3 "6+"
label values kid_agebin kid_agebin_lbl

* Compute mother-overrepresentation ratio by broad occupation
* Among employed women aged 25-45
preserve
    keep if age >= 25 & age <= 45 & employed == 1 & occ_broad < .
    
    * Share of mothers-with-young-kids in each occupation
    bysort occ_broad: egen n_mothers = total(has_young_child)
    bysort occ_broad: egen n_total = count(has_young_child)
    bysort occ_broad: egen n_childless = total(has_children == 0)
    
    gen mother_share = n_mothers / n_total
    gen childless_share = n_childless / n_total
    
    * Overrepresentation ratio
    egen total_mother_share = mean(has_young_child)
    gen overrep_ratio = mother_share / total_mother_share
    
    * Classify: high-amenity if overrep > 1.1 (mothers overrepresented)
    bysort occ_broad: keep if _n == 1
    
    list occ_broad mother_share childless_share overrep_ratio, noobs
    
    * Post results
    levelsof occ_broad, local(occs)
    foreach o of local occs {
        sum overrep_ratio if occ_broad == `o', meanonly
        if r(N) > 0 {
            post moments_handle ("F6_OCC") ///
                ("F6_mother_overrep_occ`o'") (r(mean)) (.) (.) ("ratio")
        }
    }
restore

* Flag high-amenity occupations (service + clerical as typical "flexible" jobs)
gen high_amenity_occ = (occ_broad == 4 | occ_broad == 6) if occ_broad < .
label var high_amenity_occ "In high-amenity (clerical/service) occupation"

*--- 10d. Share in high-amenity occupation around birth, by quintile ---
preserve
    keep if birth_25_35 == 1 & prebirth_quintile < .
    keep if employed == 1 & occ_broad < .
    keep if event_time >= -3 & event_time <= 8
    
    * Pre vs post birth
    gen post_birth = (event_time >= 0) if event_time < .
    
    collapse (mean) high_amenity_occ (count) N=high_amenity_occ, ///
        by(prebirth_quintile post_birth)
    
    forvalues q = 1/5 {
        forvalues pb = 0/1 {
            local plab = cond(`pb'==1, "post", "pre")
            sum high_amenity_occ if prebirth_quintile==`q' & post_birth==`pb', ///
                meanonly
            if r(N) > 0 {
                local mn = r(mean)
                sum N if prebirth_quintile==`q' & post_birth==`pb', meanonly
                local n = r(mean)
                local s = sqrt(`mn' * (1 - `mn') / `n')
                post moments_handle ("F6_OCC") ///
                    ("F6_hiamen_share_q`q'_`plab'") (`mn') (`s') (`n') ("share")
            }
        }
    }
restore

*--- 10e. High-amenity share by youngest child age bin × LE tercile ---
* (Block C partial: 9 moments)
preserve
    keep if employed == 1 & occ_broad < . & has_children == 1
    keep if kid_agebin < . & le_quintile < .
    keep if age >= 25 & age <= 50
    
    * Collapse LE quintiles to terciles for this analysis
    gen le_tercile = .
    replace le_tercile = 1 if le_quintile <= 2
    replace le_tercile = 2 if le_quintile == 3
    replace le_tercile = 3 if le_quintile >= 4
    
    collapse (mean) high_amenity_occ (count) N=high_amenity_occ, ///
        by(le_tercile kid_agebin)
    
    forvalues t = 1/3 {
        forvalues kb = 1/3 {
            local klab : label kid_agebin_lbl `kb'
            sum high_amenity_occ if le_tercile == `t' & kid_agebin == `kb', ///
                meanonly
            if r(N) > 0 {
                local mn = r(mean)
                sum N if le_tercile == `t' & kid_agebin == `kb', meanonly
                local n = r(mean)
                local s = sqrt(`mn' * (1 - `mn') / `n')
                post moments_handle ("C_partial") ///
                    ("C_hiamen_tercile`t'_kid`klab'") (`mn') (`s') (`n') ("share")
            }
        }
    }
restore

*--- 10f. Occupation transition rates around birth ---
* Track: did woman change broad occupation from pre-birth to post-birth?
preserve
    keep if birth_25_35 == 1 & prebirth_quintile < .
    keep if occ_broad < .
    
    * Pre-birth occupation (latest observation at event_time -1 or -2)
    gen occ_pre = occ_broad if event_time >= -2 & event_time <= -1
    gen occ_post = occ_broad if event_time >= 1 & event_time <= 3
    
    bysort ID: egen occ_before = mode(occ_pre), minmode
    bysort ID: egen occ_after  = mode(occ_post), minmode
    
    bysort ID: keep if _n == 1
    keep if occ_before < . & occ_after < .
    
    * Did occupation change?
    gen occ_changed = (occ_before != occ_after)
    
    * Did woman move TO high-amenity?
    gen moved_to_hiamen = (occ_changed == 1 & ///
        (occ_after == 4 | occ_after == 6) & ///
        occ_before != 4 & occ_before != 6)
    
    forvalues q = 1/5 {
        * Occupation change rate
        sum occ_changed if prebirth_quintile == `q', meanonly
        if r(N) >= 20 {
            local mn = r(mean)
            local n  = r(N)
            local s  = sqrt(`mn'*(1-`mn')/`n')
            post moments_handle ("F6_OCC") ///
                ("F6_occ_change_rate_q`q'") (`mn') (`s') (`n') ("rate")
        }
        
        * Rate of moving to high-amenity
        sum moved_to_hiamen if prebirth_quintile == `q', meanonly
        if r(N) >= 20 {
            local mn = r(mean)
            local n  = r(N)
            local s  = sqrt(`mn'*(1-`mn')/`n')
            post moments_handle ("F6_OCC") ///
                ("F6_move_to_hiamen_q`q'") (`mn') (`s') (`n') ("rate")
        }
    }
restore


/*==============================================================================
  SECTION 11: Mother vs. Non-Mother Earnings Gap by Age
  Cross-sectional child penalty: gap between mothers and age-matched
  childless women at each age.
==============================================================================*/

di ""
di "=========================================="
di "  Mother vs. Non-Mother Earnings Gap"
di "=========================================="

preserve
    keep if age >= 25 & age <= 55
    keep if has_earn == 1
    
    gen log_earn = ln(earn_real)
    
    * Mean log earnings by mother status × 5-year age bins
    gen age_bin = .
    replace age_bin = 1 if age >= 25 & age <= 29
    replace age_bin = 2 if age >= 30 & age <= 34
    replace age_bin = 3 if age >= 35 & age <= 39
    replace age_bin = 4 if age >= 40 & age <= 44
    replace age_bin = 5 if age >= 45 & age <= 49
    replace age_bin = 6 if age >= 50 & age <= 55
    
    label define age_bin_lbl 1 "25-29" 2 "30-34" 3 "35-39" ///
        4 "40-44" 5 "45-49" 6 "50-55"
    label values age_bin age_bin_lbl
    
    * Compute gap
    collapse (mean) log_earn (sd) sd_le=log_earn (count) N=log_earn, ///
        by(has_children age_bin)
    
    * Reshape to compute gap
    reshape wide log_earn sd_le N, i(age_bin) j(has_children)
    
    gen mother_penalty = log_earn1 - log_earn0   // mother - childless
    gen se_gap = sqrt(sd_le1^2/N1 + sd_le0^2/N0)
    
    forvalues ab = 1/6 {
        sum mother_penalty if age_bin == `ab', meanonly
        if r(N) > 0 {
            local mn = r(mean)
            sum se_gap if age_bin == `ab', meanonly
            local s = r(mean)
            sum N1 if age_bin == `ab', meanonly
            local n = r(mean)
            
            local alab : label age_bin_lbl `ab'
            post moments_handle ("MOTHER_GAP") ///
                ("mothergap_age`alab'") (`mn') (`s') (`n') ("log_diff")
        }
    }
    
    * Also by education
restore

preserve
    keep if age >= 25 & age <= 55 & has_earn == 1 & college < .
    gen log_earn = ln(earn_real)
    
    collapse (mean) log_earn (count) N=log_earn, ///
        by(has_children college)
    
    reshape wide log_earn N, i(college) j(has_children)
    gen gap = log_earn1 - log_earn0
    
    forvalues ed = 0/1 {
        local edlab = cond(`ed'==1, "college", "noncollege")
        sum gap if college == `ed', meanonly
        if r(N) > 0 {
            sum N1 if college == `ed', meanonly
            local n = r(mean)
            post moments_handle ("MOTHER_GAP") ///
                ("mothergap_`edlab'") (r(mean)) (.) (`n') ("log_diff")
        }
    }
restore


/*==============================================================================
  SECTION 12: Lifecycle Earnings Profiles (PSID analog of Block A)
  Not GKOS SSA moments, but PSID-based sanity check.
==============================================================================*/

di ""
di "=========================================="
di "  Lifecycle Earnings Profiles (PSID)"
di "=========================================="

preserve
    keep if age >= 25 & age <= 55 & has_earn == 1
    keep if le_quintile < .
    
    gen log_earn = ln(earn_real)
    
    * Mean log earnings by LE quintile × 5-year age bins
    gen age5 = 5 * floor(age / 5)
    replace age5 = 25 if age5 < 25
    replace age5 = 50 if age5 > 50
    
    collapse (mean) log_earn (count) N=log_earn, ///
        by(le_quintile age5)
    
    * Post as descriptive lifecycle profiles
    forvalues q = 1/5 {
        levelsof age5 if le_quintile == `q', local(ages)
        foreach a of local ages {
            sum log_earn if le_quintile == `q' & age5 == `a', meanonly
            if r(N) >= 20 {
                sum N if le_quintile == `q' & age5 == `a', meanonly
                local n = r(mean)
                post moments_handle ("A_PSID") ///
                    ("A_psid_logearn_q`q'_age`a'") (r(mean)) (.) (`n') ("log_USD")
            }
        }
    }
restore


/*==============================================================================
  SECTION 13: Descriptive statistics
==============================================================================*/

di ""
di "=========================================="
di "  Descriptive Statistics"
di "=========================================="

*--- Total women in sample ---
preserve
    bysort ID: keep if _n == 1
    count
    post moments_handle ("DESC") ("total_women") (r(N)) (.) (.) ("count")
    
    count if first_bio_birth_year < .
    post moments_handle ("DESC") ("women_with_births") (r(N)) (.) (.) ("count")
    
    count if birth_25_35 == 1
    post moments_handle ("DESC") ("women_birth_25_35") (r(N)) (.) (.) ("count")
    
    count if prebirth_quintile < .
    post moments_handle ("DESC") ("women_with_quintile") (r(N)) (.) (.) ("count")
    
    sum age_first_birth if birth_25_35 == 1
    post moments_handle ("DESC") ("mean_age_first_birth_25_35") ///
        (r(mean)) (r(sd)/sqrt(r(N))) (r(N)) ("years")
    
    sum n_bio_children
    post moments_handle ("DESC") ("mean_bio_children_all") ///
        (r(mean)) (r(sd)/sqrt(r(N))) (r(N)) ("count")
    
    sum avg_prebirth if prebirth_quintile < ., d
    post moments_handle ("DESC") ("median_prebirth_earn") ///
        (r(p50)) (.) (r(N)) ("USD2024")
restore

/*==============================================================================
  SECTION 14: Close postfile and export to CSV
==============================================================================*/

postclose moments_handle

*--- Load and export moments ---
use `moments_file', clear

* Label columns
label var block       "Moment block"
label var moment_name "Moment identifier"
label var value       "Point estimate"
label var se          "Standard error"
label var N           "Sample size"
label var unit        "Unit of measurement"

* Sort
gsort block moment_name

* Display
list, noobs sep(10) abbreviate(20)

* Export to CSV
export delimited using "${outdir}/moments_psid_shelf.csv", replace
di "Moments saved to: ${outdir}/moments_psid_shelf.csv"

*--- Summary of what was computed ---
di ""
di "=========================================="
di "  MOMENTS SUMMARY"
di "=========================================="

tab block

di ""
di "COMPUTABLE WITH SHELF (done above):"
di "  Block B: Earnings path (40) + E-to-U (5) = 45 moments"
di "  Block C: High-amenity occ share by kid age × tercile = 9 moments (partial)"
di "  Block D: Age 1st birth (5) + total children (5) +"
di "           frac child@25 (2) + emp@25 (2) +"
di "           earn@25 (2) + earn growth (2) = 18 moments"
di "  Block E: Recovery t+3,t+5 × 3 terciles = 6 moments"
di "  Fact 3:  State PFL DiD (earnings + employment, CA event study)"
di "  Fact 6:  Occupation sorting (overrep ratios, transition rates,"
di "           high-amenity shares pre/post birth by quintile)"
di "  Extra:   Long-horizon recovery, Kleven event study,"
di "           mother/non-mother gap by age, lifecycle profiles"
di ""
di "NOT COMPUTABLE (need supplemental data):"
di "  Block A:  GKOS SSA lifecycle earnings (60 moments)"
di "  Block B:  Hourly wage change at birth (5 moments)"
di "           [need annual hours + wage rate from PSID Data Center]"
di "  Block C:  Annual hours by LE × pre/post (6 moments)"
di "           [need annual hours from Data Center; have partial occ sorting]"
di "  Block F:  GKOS SSA arc-percent moments (36 moments)"
di "  Fact 2:  FMLA threshold analysis"
di "           [need employer size from Data Center carts]"
di "  Fact 5:  Hours vs. wage-rate decomposition"
di "           [need annual hours + wage rate from Data Center carts]"
di ""

log close

di ""
di "=========================================="
di "  DONE"
di "=========================================="
