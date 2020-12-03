#! python3

#   Cellfile creation for Net Monitor
#   format of command is >python CellFile_NetMon.py  UMC_Atoll_DB_KIE_4G.mdb

import pyodbc, math, datetime
from tqdm import tqdm


r = 6378150  # Earth radius, m
pi180 = math.pi / 180


def coord_calc_DxDy(lon1, lat1, dx, dy):
    # coordinates lon2,lat2 base on Dx/Dy distance from point lon1,lat1

    arccos = ((math.cos(dx / r) - (math.sin(lat1 * pi180)) ** 2) / ((math.cos(lat1 * pi180)) ** 2))

    if arccos >= 1:
        arc = 0
    elif arccos <= -1:
        arc = math.pi
    else:
        arc = math.atan((-arccos) / math.sqrt(-arccos * arccos + 1)) + 1.5707963267949

    lat2 = lat1 + dy / r / pi180

    if dx > 0:
        lon2 = lon1 + arc / pi180
    else:
        lon2 = lon1 - arc / pi180

    return lon2, lat2


def mdbGQueryData(MDB):
    # getting query of GSM cell info from Atoll *.mdb

    DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'

    # connect to db
    con = pyodbc.connect('DRIVER={};DBQ={}'.format(DRV, MDB))
    cur = con.cursor()
    cur.execute("""SELECT
                        sites.NAME,
                        sites.LONGITUDE,
                        sites.LATITUDE,
                        gtransmitters.CONTROL_CHANNEL,
                        gtransmitters.LAC,
                        gtransmitters.Dx,
                        gtransmitters.Dy,
                        gtransmitters.tx_id,
                        gtransmitters.azimut,
                        gtransmitters.HEIGHT,
                        gtransmitters.TILT,
                        gtransmitters.ANTENNA_NAME,
                        gtransmitters.CELL_IDENTITY

                        FROM sites INNER JOIN gtransmitters ON sites.NAME = gtransmitters.SITE_NAME

                        WHERE (((gtransmitters.CONTROL_CHANNEL) Is Not Null))

                        ORDER BY gtransmitters.tx_id;""")
    # ('KIE_YRO_VKA', 30.354166666666668, 50.34166666666667, 62, 1824, 0.0, 0.0, 'KIE_YRO_VKA_275_G', 275.0, 37.0, 0.0, 'ATR4518R7v06_0900_02', 1704)

    mdbdata = cur.fetchall()
    cur.close()
    con.close()

    for i in tqdm(mdbdata, desc='GSM   Cells', unit=' cells'):  # progressbar
        #   for i in mdbdata:
        # 1,255,1,1911,,18387,,D1800,806,KIE_BER_ITK,50.27486111,31.46891667,KIE_BER_ITK_310_D,310,25,0,6

        lt2 = str(coord_calc_DxDy(i[1], i[2], i[5], i[6])[1])
        ln2 = str(coord_calc_DxDy(i[1], i[2], i[5], i[6])[0])
        if i[3] > 200:
            bnd = 'D1800'
        else:
            bnd = 'G900'
        rfcn = str(i[3])

        try:
            tlt = str(int(i[11][-2:]))
        except:
            tlt = '00'

        row = ';'.join(
            ('1', '255', '1', str(i[4]), '', str(i[12]), '', bnd, rfcn, i[0], lt2,
             ln2, str(i[7]), str(int(i[8])), str(int(i[9])), str(int(i[10])), tlt))
        cf.write(row + '\n')


def mdbWQueryData(MDB):
    # getting query of WCDMA cell info from Atoll *.mdb

    DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'

    # connect to db
    con = pyodbc.connect('DRIVER={};DBQ={}'.format(DRV, MDB))
    cur = con.cursor()
    cur.execute("""SELECT
                        sites.NAME,
                        sites.LONGITUDE,
                        sites.LATITUDE,
                        sites.RNC,
                        utransmitters.LAC,
                        utransmitters.Dx,
                        utransmitters.Dy,
                        utransmitters.tx_id,
                        utransmitters.azimut,
                        utransmitters.HEIGHT,
                        utransmitters.TILT,
                        utransmitters.ANTENNA_NAME,
                        ucells.CARRIER,
                        ucells.CELL_IDENTITY,
                        ucells.SCRAMBLING_CODE

                        FROM sites INNER JOIN (utransmitters INNER JOIN ucells ON utransmitters.TX_ID = ucells.TX_ID) ON sites.NAME = utransmitters.SITE_NAME

                        WHERE (((ucells.SCRAMBLING_CODE) Is Not Null))
                        
                        ORDER BY utransmitters.tx_id;""")
    # ('KIE_ZAZ_LES', 30.693805555555556, 50.56225, 'RNC-1911', 1853, 0.0, 0.0, 'KIE_ZAZ_LES_315_U', 315.0, 25.0, 0.0, 'INT-1900-19-5-65_2100_02', 2, '25244', 502)

    mdbdata = cur.fetchall()
    cur.close()
    con.close()

    for i in tqdm(mdbdata, desc='WCDMA Cells', unit=' cells'):  # progressbar
        # 2;255;1;1856;1811;46137;444;U2100;10762;KIE_KIE_MSK;50.43703179;30.54194444;KIE_KIE_MSK_060_U;60;28;2;8

        lt2 = str(coord_calc_DxDy(i[1], i[2], i[5], i[6])[1])
        ln2 = str(coord_calc_DxDy(i[1], i[2], i[5], i[6])[0])
        bnd = 'U2100'
        rfcn = str((i[12] - 1) * 25 + 10712)

        try:
            tlt = str(int(i[11][-2:]))
        except:
            tlt = '00'

        try:
            mtlt = str(int(i[10]))
        except:
            mtlt = '00'

        try:
            azim = str(int(i[8]))
        except:
            azim = '00'

        try:
            row = ';'.join(
                ('2', '255', '1', str(i[4]), str(i[3])[-4:], str(i[13]), str(i[14]), bnd, rfcn, str(i[0]), lt2,
                 ln2, str(i[7]), azim, str(int(i[9])), mtlt, tlt))
        except:
            print('Exception raised in row!!!!\n', *i)
            continue

        if i[3]:  # if RNCId is exist
            cf.write(row + '\n')


def mdbLQueryData(MDB):
    # getting query of LTE cell info from Atoll *.mdb

    DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'

    # connect to db
    con = pyodbc.connect('DRIVER={};DBQ={}'.format(DRV, MDB))
    cur = con.cursor()
    cur.execute("""SELECT
                        sites.NAME,
                        sites.LONGITUDE,
                        sites.LATITUDE,
                        ltransmitters.lnBtsId,    
                        ltransmitters.TAC,
                        ltransmitters.Dx,
                        ltransmitters.Dy,
                        ltransmitters.Lcrid,
                        ltransmitters.freq,
                        ltransmitters.tx_id,
                        ltransmitters.azimut,
                        ltransmitters.HEIGHT,
                        ltransmitters.TILT,
                        ltransmitters.ANTENNA_NAME,
                        lcells.PHY_CELL_ID
                        
                        FROM sites INNER JOIN (ltransmitters INNER JOIN lcells ON ltransmitters.TX_ID = lcells.TX_ID) ON sites.NAME = ltransmitters.SITE_NAME
                        
                        WHERE (((lcells.PHY_CELL_ID) Is Not Null))
                        
                        ORDER BY ltransmitters.tx_id;""")
    # ('KIE_YAG_KUZ', 31.775, 50.244194444444446, 430124, 1911, 0.0, 0.0, 31, 1800, 'KIE_YAG_KUZ_350_L18', 350.0, 50.0, 0.0, 'APXVRR20-C_1800_04', 153)

    mdbdata = cur.fetchall()
    cur.close()
    con.close()

    for i in tqdm(mdbdata, desc='LTE   Cells', unit=' cells'):  # progressbar
        # 3,255,1,1856,590184,31,196,L1800,1700,KIE_KIE_MSK,50.43703179,30.54194444,KIE_KIE_MSK_060_L18,60,28,2,8

        lt2 = str(coord_calc_DxDy(i[1], i[2], i[5], i[6])[1])
        ln2 = str(coord_calc_DxDy(i[1], i[2], i[5], i[6])[0])
        bnd = 'L' + str(i[8])
        if i[8] > 2000:
            rfcn = '2900'
        else:
            rfcn = '1700'

        try:
            tlt = str(int(i[13][-2:]))
        except:
            tlt = '00'
        row = ';'.join(
            ('3', '255', '1', str(i[4]), str(i[3]), str(i[7]), str(i[14]), bnd, rfcn, str(i[0]), lt2,
             ln2, str(i[9]), str(int(i[10])), str(int(i[11])), str(int(i[12])), tlt))
        cf.write(row + '\n')


if __name__ == '__main__':
    now = datetime.datetime.now()
    # print (now.strftime("%Y-%m-%d %H:%M:%S"))
    database = 'T:/UMC_Atoll_DB_KIE_v332.mdb'
    cellfile = 'CF_NetMon_KIE_' + now.strftime("%Y%m%d") + '.csv'

    print('\n', 'Cooking CellFile for NetMonitor!', '\n', 'Processing database', database, '\n')

    with open(cellfile, 'w') as cf:
        # writing header of cellfile
        cf.write(
            'tech;mcc;mnc;lac_tac;node_id;cid;psc_pci;band;arfcn;site_name;cell_lat;cell_long;cell_name;azimuth;height;tilt_mech;tilt_el\n')

    with open(cellfile, 'a') as cf:
        mdbGQueryData(database)
        mdbWQueryData(database)
        mdbLQueryData(database)

    print('\n' + cellfile, 'generated!')
