"""
Write both valid and corrupt bitmap points in JSON format
"""

import numpy as np
np.set_printoptions(threshold=np.nan, linewidth=300)

def create_single_char(letter):
    b = 15    	# border
    m = b / 2 	# middle
    s = 3	# letter starts at
    e = b - 3   # letter ends at
    d = np.zeros((b,b), dtype=np.int)

    if (letter == 'I'):
        for y in range(s, e):
            d[y, m] = 1
        for x in range(s, e):
            d[2, x] = 1
            d[e, x] = 1
    elif (letter == 'T'):
        for y in range(s, e+1):
            d[y, m] = 1
        for x in range(s, e):
            d[2, x] = 1
    elif (letter == 'A'):
        for y in range(m+2, e-1):
            d[y, 4] = 1
        for y in range(m, e+1):
            d[y, e-2] = 1
        for x in range(s+3, e-1):
            d[m, x] = 1
            d[e, x] = 1
        # fine tune for b=11
            d[8, 5] = 1               
            d[11, 5] = 1               
    elif (letter == 'B'):
        for y in range(s-1, e+1):
            d[y, s] = 1
        for x in range(s+1, e-4):
            d[s-1, x] = 1
            d[e, x] = 1
        for x in range(s+1, m):
            d[m, x] = 1
        # fine tune for b=11
	    d[3,8] = 1
	    d[4,8] = 1
	    d[5,8] = 1
	    d[6,7] = 1
	    d[8,7] = 1
	    d[9,8] = 1
	    d[10,8] = 1
	    d[11,8] = 1
    elif (letter == 'G'):
        # too curvy, fine tune everything
            d[2,10] = 1
            d[2,9] = 1
            d[2,8] = 1
            d[2,7] = 1
            d[3,6] = 1
            d[3,5] = 1
            d[4,4] = 1
            d[4,3] = 1
            d[5,2] = 1
            d[6,2] = 1
            d[7,2] = 1
            d[8,2] = 1
            d[9,3] = 1
            d[10,4] = 1
            d[11,5] = 1
            d[12,6] = 1
            d[12,7] = 1
            d[11,8] = 1
            d[10,9] = 1
            d[9,10] = 1
            d[8,11] = 1
            d[7,11] = 1
            d[7,10] = 1
            d[7,9] = 1
            d[7,8] = 1
            d[7,7] = 1
    elif (letter == 'D'):
        for y in range(s-1, e+1):
            d[y, s] = 1
        for x in range(s+1, s+5):
            d[s-1, x] = 1
            d[e, x] = 1
        for x in range(s+5, s+7):
            d[s, x] = 1
            d[e-1, x] = 1
        # fine tune for b=11
	    d[4, 10] = 1
	    d[5, 10] = 1
	    d[6, 11] = 1
	    d[7, 11] = 1
	    d[8, 11] = 1
	    d[9, 10] = 1
	    d[10, 10] = 1
    else:
        print("no match")
	return None
    return d

def insert_letter(full, letter, x_offset, y_offset):
    letter_bitmap = create_single_char(letter)
    if letter_bitmap is not None:
        for y in range(len(letter_bitmap)):
            for x in range(len(letter_bitmap)):
                full[y+y_offset][x + x_offset] = letter_bitmap[y][x]

def write_csv_file(full):

    #  order is y:x
    # corners
    full[0][0] = 1;
    full[0][1] = 1;
    full[0][2] = 1;
    full[1][0] = 1;
    full[1][1] = 1;
    full[1][2] = 1;
    full[4][2] = 1;
    full[4][3] = 1;
    full[4][4] = 1;
    full[4][5] = 1;
    full[5][1] = 1;
    full[5][2] = 1;
    full[5][3] = 1;
    full[5][4] = 1;
    full[5][5] = 1;
    full[0][119] = 1;
    full[1][119] = 1;
    full[2][119] = 1;
    full[3][119] = 1;
    full[4][119] = 1;
    full[5][119] = 1;
    full[6][119] = 1;
    full[7][119] = 1;
    full[8][119] = 1;
    full[9][119] = 1;
    full[119][0] = 1;
    full[119][1] = 1;
    full[119][2] = 1;
    full[119][3] = 1;
    full[119][4] = 1;
    full[119][5] = 1;
    full[119][9] = 1;
    full[119][8] = 1;
    full[119][119] = 1;
    full[59][117] = 1;
    full[59][2] = 1;
    full[2][52] = 1;
    full[118][52] = 1;
    full[119][56] = 1;
    full[119][62] = 1;
    full[119][63] = 1;
    full[119][64] = 1;
    for step in range(1, 119):
        full[step][1] = 1;
        full[step][2] = 1;
        full[step][3] = 1;
        full[step][4] = 1;
        full[step][119] = 1;
        full[step][118] = 1;
        full[step][115] = 1;
        full[step][114] = 1;
        full[step][113] = 1;
        full[2][step] = 1;
        full[118][step] = 1;
        full[117][step] = 1;
        full[116][step] = 1;
    # noise
    #full[30][80] = 1;
    #full[30][82] = 1;
    full[32][82] = 1;
    full[40][70] = 1;
    full[50][60] = 1;
    #full[80][30] = 1;
    #full[82][30] = 1;
    full[50][85] = 1;
    full[20][75] = 1;
    full[60][65] = 1;
    full[60][35] = 1;
    full[70][20] = 1;
    full[20][70] = 1;
    full[100][70] = 1;
    full[100][100] = 1;
    full[85][77] = 1;
    full[20][73] = 1;
    full[43][73] = 1;
    full[8][33] = 1;
    full[100][73] = 1;


    target = open('../output_files/out_s1/out_s1.json', 'w')
    target.write("x,y,val\n")
    sn = 1
    for y in range(len(full)):
        for x in range(len(full)):
            target_val = full[119-y][x]
            if (target_val == 1):
            	my_str = "{\"point\" : [ {\"sn\" : \"" + \
		    str(sn) + "\"}, {\"x\" : \"" +  str(x) + \
                    "\"}, {\"y\" : \"" + str(y) + \
                    "\"} ] }\n";
            	target.write(my_str)
                sn += 1

   # example output
   # {"point" : [ {"sn" : "186"}, {"x" : "null"}, {"y" : "119"} ] }
    
    
    # insert some null values
    target.write("{\"point\" : [ {\"sn\" : \"188\"}, {\"x\" : \"null\"}, {\"y\" : \"119\"} ] }\n")

    # insert some corrupt partial statements
    target.write("{\"point\" : [ {\"sn\" : \"146\"}, {\"x\" : \"59\"}, {\n")
    target.write("{\"point\" : [ {\"sn\" : \"111\"}, {\"x}, \n")
    target.write("{\"point\" : [ {\"sn\" : \"122\n")

    target.close()

def create_full_bitmap():
    full = np.zeros((120,120), dtype=np.int)
    orig_col = 20
    col = orig_col + 10
    inc = 22
    insert_letter(full, 'B', col, 35)
    col += inc
    insert_letter(full, 'I', col, 35)
    col += inc
    insert_letter(full, 'G', col, 35)

    col = orig_col
    insert_letter(full, 'D', col, 75)
    col += inc
    insert_letter(full, 'A', col, 75)
    col += inc
    insert_letter(full, 'T', col, 75)
    col += inc
    insert_letter(full, 'A', col, 75)

    #print str(full).replace(' ', '').replace('0', '.').replace(']', '').replace('[', '')
    write_csv_file(full)

create_full_bitmap()
