"""
flood_detection.py

Adapted from Cloud2Street's MODIS flood detection:
https://github.com/cloudtostreet/MODIS_GlobalFloodDatabase/blob/main/flood_detection/modis.py

Modifications:
-------------
- Changed 3-day composite to center on the event day using ±1 day (instead of previous 2 days).
- `clear_perc_scaled` now returns an integer scaled 0–100, instead of float 0–1 (previously 'clear_perc'),
  to ensure consistent band types for Earth Engine export.
- Bounding box (roi.bounds()) is no longer computed inside the function; caller must now precompute
  and provide the appropriate roi

Inputs:
-------
- roi (ee.Geometry): Region of interest.
- began (str): Event start date (YYYY-MM-DD).
- ended (str): Event end date (YYYY-MM-DD).
- threshold (str): Thresholding method; "standard" or "otsu".
- get_max (bool): If True, includes the maximum flood extent as an additional band.

Outputs:
--------
ee.Image with 4 bands:
    - 'flooded': Binary flood extent (1 = flooded, 0 = not flooded).
    - 'duration': Number of flooded days at each pixel.
    - 'clear_views': Number of clear-sky observations.
    - 'clear_perc_scaled': Percent of clear views (0–100).
"""

import ee
from . import modis_toolbox


def detect_flooded_pixels(roi, began, ended, threshold, get_max=False):
    # Get dates as ee.Date()
    # For a 3-day composite, take the day +/- 1 day (one day before+after) for the event duration
    # Need to advance 2 instead of 1 due to exclusive upper bound
    date_range = ee.DateRange(
        ee.Date(began).advance(-1, "day"), ee.Date(ended).advance(2, "day")
    )

    def region_clip(img):
        return img.clip(roi)

    # STEP 2 - LOAD IMPORTANT MODIS DATA BASED ON DATES
    # Collect Terra and Aqua satellites
    terra = modis_toolbox.get_terra(roi, date_range).map(region_clip)
    aqua = modis_toolbox.get_aqua(roi, date_range).map(region_clip)

    # Apply Pan-sharpen function to aqua and terra images
    terra_sharp = terra.map(modis_toolbox.pan_sharpen)
    aqua_sharp = aqua.map(modis_toolbox.pan_sharpen)

    # add NIR/RED ratio to all images
    terra_ratio = terra_sharp.map(modis_toolbox.b1b2_ratio)
    aqua_ratio = aqua_sharp.map(modis_toolbox.b1b2_ratio)

    # Apply QA Band Extract to Terra & Aqua
    terra_final = terra_ratio.map(modis_toolbox.add_qa_bands)
    aqua_final = aqua_ratio.map(modis_toolbox.add_qa_bands)

    # Finally, the Terra and Aqua products are combined into one image
    # collection so they can be accessed in together in the DFO algorithm.
    modis = ee.ImageCollection(
        terra_final.merge(aqua_final).sort("system:time_start", True)
    )
    print("Collected and pre-processed MODIS Images")

    # STEP 3 - APPLY THE DFO WATER DETECTION & COMPOSITING ALGORITHMS
    # Okay - this is where it starts to get exciting.  The portion of the code
    # applies the DFO algorithm by first optimizing the thresholds applied to
    # water detection, then applying those thresholds, and finally compositing
    # images based on a 3-day window. The "otsu" mode uses bimodal histogram
    # splitting on the bands to select thresholds, or the "standard" mode uses
    # the static thresholds from DFO

    # STEP 3.1 - SELECT thresholds
    if threshold == "standard":
        thresh_dict = {"b1b2": 0.70, "b7": 675.00, "base_res": None}
    elif threshold == "otsu":
        # Apply the qa_mask to each modis image.  We then make a composite across
        # all the flood images to one image.  This is done so to increase the
        # sampling space as well as represent variation within the flood event
        # itself.  Clip to the roi to exclude ocean area in the sample.
        modis_masked = modis.map(modis_toolbox.qa_mask)
        sample_frame = modis_masked.median().clip(roi)

        # Get a watermask that can be used to define strata for sampling
        strata = (
            modis_toolbox.get_jrc_yearly_perm(began, roi)
            .updateMask(sample_frame.select("red_250m").mask())
            .int8()
            .clip(roi)
        )

        # Otsu histrograms require a "bi-modal histogram". We need to constrain
        # the reflectance range that can be used in the histogram as it may
        # include high-reflectance features (e.g. missed clouds) that will make
        # the histogram "multi-modal". Below are the steps to constrain the
        # histograms into a reasonable range that one might expect water/ land
        swir_mask = (
            sample_frame.select("swir")
            .gt(-500)
            .And(sample_frame.select("swir").lt(3000))
        )
        cleaned_swir = sample_frame.select("swir").updateMask(swir_mask)

        # Put it all together in a final sample image
        sample_img = sample_frame.addBands(strata).addBands(
            cleaned_swir, overwrite=True
        )

        # Collect Sample using stratifiedSample() function
        sample_bands = ["b1b2_ratio", "swir", "jrc_perm_yearly"]
        base_res = (
            ee.Image(modis.first())
            .select("red_250m")
            .projection()
            .nominalScale()
            .multiply(1)
            .getInfo()
        )
        base_res = round(base_res, 2)
        sample = sample_img.select(sample_bands).stratifiedSample(
            numPoints=2500,
            classBand="jrc_perm_yearly",
            region=roi,
            scale=base_res,
            dropNulls=True,
        )

        # Convert the sample to a histogram
        b1b2_hist = sample.reduceColumns(ee.Reducer.histogram(), ["b1b2_ratio"]).get(
            "histogram"
        )
        swir_hist = sample.reduceColumns(ee.Reducer.histogram(), ["swir"]).get(
            "histogram"
        )

        # Calculate histogram, run otsu, and collect into a dictionary
        b1b2_thresh = modis_toolbox.otsu_get_threshold(b1b2_hist)
        swir_thresh = modis_toolbox.otsu_get_threshold(swir_hist)
        thresh_dict = {
            "b1b2": b1b2_thresh.getInfo(),
            "b7": swir_thresh.getInfo(),
            "base_res": base_res,
        }

        print("Calculated thresholds for Otsu: {0}".format(thresh_dict))

    else:
        raise ValueError("'threshold' options are 'standard' or 'otsu'")

    # STEP 3.2 - APPLY THRESHOLDS TO MODIS IMAGES
    # The following function cycles through each MODIS image prepared above
    # and applies the thresholds to distinguish land from water. To do this
    # Bands 1, 2, and 7 (Red, NIR, and SWIR) are used.  Within the function a
    # Band 2/ Band 1 ratio is defined.  The thresholds are combined and where a
    # pixel passes all three thresholds it is flagged as water.
    def dfo_water_detection(modis_collection, thresh_b1b2, thresh_b7):
        def water_flag(img):
            # Apply thresholds to each ratio/ band
            b1b2_ratio = ee.Image(img.select("b1b2_ratio"))
            b1b2_sliced = b1b2_ratio.lt(ee.Image.constant(thresh_b1b2))  # Band 1/Band 2
            b1_sliced = img.select(["red_250m"], ["b1_thresh"]).lt(
                ee.Image.constant(2027)
            )  # Band 1 Threshold
            b7_sliced = img.select(["swir"], ["b7_thresh"]).lt(
                ee.Image.constant(thresh_b7)
            )  # Band 7 Threshold

            # Add all the thresholds to one image and then sum()
            thresholds = b1b2_sliced.addBands(b1_sliced).addBands(b7_sliced)
            thresholds_count = thresholds.reduce(ee.Reducer.sum())

            # Apply water_flage threshold to final image
            water_flag = thresholds_count.gte(ee.Image.constant(3))
            return water_flag.copyProperties(img).set(
                "system:time_start", img.get("system:time_start")
            )

        # Apply the 'water_flag' function over the modis collection
        dfo_water_collection = modis_collection.map(water_flag)
        return dfo_water_collection.set(
            {
                "threshold_b1b2": round(thresh_b1b2, 3),
                "threshold_b7": round(thresh_b7, 2),
                "otsu_sample_res": thresh_dict["base_res"],
            }
        )

    # The dfoWaterDetection() function is mapped over the MODIS collection
    modis_dfo_water_detection = dfo_water_detection(
        modis, thresh_dict["b1b2"], thresh_dict["b7"]
    )

    # STEP 3.2 - DFO COMPOSITES
    # The following functions create a 3-day composites.  IMPORTANT:
    # This is done by using a join where a lag period is defined (in milliseconds)
    # where images +/- 1 day are joined to the current image as a
    # property.  This is later extracted in a function to access the images
    # and create a composite.

    def join_surrounding_days(collection, half_window_days):
        """
        Join images within ±half_window_days from each image in the collection.
        Stores the matched images in a 'dfo_images' property.
        """
        # Convert days to milliseconds
        # So if half_window_days = 1, this will be one day
        max_diff_ms = 1000 * 60 * 60 * 24 * half_window_days

        # Create a filter that matches images whose acquisition times are within
        # ±half_window_days (converted to milliseconds) of each other.
        filt = ee.Filter.maxDifference(
            difference=max_diff_ms,
            leftField="system:time_start",
            rightField="system:time_start",
        )

        # Perform a self-join using the filter above. For each image in the collection,
        # find and attach all matching images (within the time window) in a property called 'dfo_images'.
        # The matches are ordered by time to ensure consistent composite construction.
        return ee.Join.saveAll(
            matchesKey="dfo_images", ordering="system:time_start", ascending=True
        ).apply(collection, collection, filt)

    # Apply the composite to create a 3 day window for each day
    modis_join = join_surrounding_days(modis_dfo_water_detection, half_window_days=1)

    # The next function takes the join_previous_days results and combines the
    # images in order to create the actual composite.  This is done by
    # accessing the images stored in the properties of each image (i.e. the
    # images 1 or 2 days prior).  Each composite has 2x the number of
    # images as it does days since we use both Terra and Aqua.  Where at
    # least half of those days are flagged as water a pixel is marked as
    # water.  This step help avoid marking cloud shadows, that move between
    # images, as water.  A common misclassification in these types of
    # algorithms.

    def dfo_flood_water(composite_collection, comp_days):
        def apply_comp_day(image):
            dfo_composite = ee.ImageCollection.fromImages(image.get("dfo_images")).sum()
            stable_water_thresh = dfo_composite.gte(comp_days)
            return (
                stable_water_thresh.select(["sum"], ["flood_water"])
                .copyProperties(image)
                .set({"system:time_start": image.get("system:time_start")})
            )

        stable_water_thresh = composite_collection.map(apply_comp_day)
        return stable_water_thresh.set(
            {"composite_type": ee.String(str(comp_days)).cat("Day")}
        )

    # If the began date is before Aqua started, change the critera for flooded pixels
    # Since there will be half the images available
    #
    # Terra & Aqua (post 2002-07-04)
    # DFO Threshold for flood water is 3 for 3-day composites
    #
    # Terra Only (pre 2002-07-04)
    # DFO Threshold for flood water is 2 for 3-day composites

    if (ee.Date(began).difference(ee.Date("2002-07-04"), "day")).gte(0):
        dfo_comp = 3
    elif (ee.Date(began).difference(ee.Date("2002-07-04"), "day")).lt(0):
        dfo_comp = 2

    dfo_flood_coll = dfo_flood_water(modis_join, dfo_comp)

    # Keep only composites where the center date is within the actual event period
    dfo_flood_coll = dfo_flood_coll.filterDate(began, ee.Date(ended).advance(1, "day"))

    # STEP 3.3 COLLAPSE COMPOSITES INTO A FINAL FLOOD MAP
    # The following function is the last step in the DFO algorithm.  Here we
    # take the resulting image collection of composites and collapse it into a
    # final flood extent and flood frequency image.  Flood extent is defined by
    # all pixels that were identified as flood in any composite.  Flood
    # frequency is the number of times a pixel was flagged as a flood pixel.
    def flood_extent_freq(img_coll):
        freq = (
            ee.ImageCollection(img_coll).sum().divide(ee.Image.constant(2)).toUint16()
        )
        flooded = freq.gte(ee.Image.constant(1))
        return ee.Image(
            flooded.select(["flood_water"], ["flooded"])
            .addBands(freq.select(["flood_water"], ["duration"]))
            .copyProperties(img_coll)
        )

    dfo_flood_img = flood_extent_freq(dfo_flood_coll)

    # STEP 3.4 CALCULATE CLEAR DAYS
    # The following function takes an imageCollection that was previously run
    # through qaBandExtract (i.e. the input band names match with those output
    # by qaBandExtract) and returns an image that calculates the number of clear
    # days for each pixel during the flood period.
    def get_clear_views(img_coll):
        def get_cloud_mask(img):
            clouds = img.select("cloud_state").eq(0)
            shadows = img.select("cloud_shadow").eq(0)
            return clouds.add(shadows).gt(0)

        clear_views = img_coll.map(get_cloud_mask)
        number_clear_views = (
            ee.Image(clear_views.sum())
            .select(["cloud_state"], ["clear_views"])
            .toUint16()
        )

        def add_obs(img):
            obs = img.select(["cloud_state"], ["observation"]).gte(0)
            return img.addBands(obs)

        observations = img_coll.map(add_obs)
        total_obs = observations.select("observation").sum()
        clear_perc = number_clear_views.divide(ee.Image(total_obs)).select(
            ["clear_views"], ["clear_perc"]
        )
        return number_clear_views.addBands(clear_perc)

    dfo_clear_days = get_clear_views(modis)

    # STEP 3.4a ADD MAX IMG
    # For the validation we want to use the image with the maximum flood extent.
    # Calculate the maxImg with the function below that runs a reduceRegion() and
    # then selects the image with the max value.
    if get_max == True:

        def get_max_img(img_coll):
            # Function to calculate the flood extent of each image
            def calc_extent(img):
                img_extent = ee.Image(img).reduceRegion(
                    reducer=ee.Reducer.sum(), geometry=roi, scale=1000, maxPixels=10e9
                )
                return img.set({"extent": img_extent.get("flood_water")})

            # Apply calcExtent() function to each image
            extent = img_coll.map(calc_extent)
            max_val = extent.aggregate_max("extent")
            max_img = ee.Image(
                extent.filterMetadata("extent", "equals", max_val).first()
            )
            date = ee.Date(max_img.get("system:time_start"))
            return max_img.select(["flood_water"], ["max_img"]).set(
                {"max_img_date": date}
            )

        max_img = get_max_img(dfo_flood_coll)
        max_img_date = ee.Date(max_img.get("max_img_date")).format("yyyy-MM-dd")

        # STEP 3.5_TRUE: PREP FINAL IMAGES
        # Add all the prepared bands together
        dfo_final = (
            ee.Image(dfo_flood_img)
            .addBands([dfo_clear_days, max_img])
            .clip(roi)
            .set(
                {
                    "began": ee.Date(began).format("yyyy-MM-dd"),
                    "ended": ee.Date(ended).format("yyyy-MM-dd"),
                    "threshold_type": threshold,
                    "max_img_date": max_img_date,
                }
            )
        )
    elif get_max == False:
        # STEP 3.5_FALSE: PREP FINAL IMAGES
        # Add all the prepared bands together
        dfo_final = (
            ee.Image(dfo_flood_img)
            .addBands(dfo_clear_days)
            .clip(roi)
            .set(
                {
                    "began": ee.Date(began).format("yyyy-MM-dd"),
                    "ended": ee.Date(ended).format("yyyy-MM-dd"),
                    "threshold_type": threshold,
                }
            )
        )

    else:
        raise ValueError("'max_img' options are 'True' or 'False'")

    print("Flood Dectection Complete")
    return dfo_final
