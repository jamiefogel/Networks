*****************************************************************
* educ_censo_2010.do
* Guilherme Hirata (PUC-Rio)
*****************************************************************

***************************************************
* Anos de estudo - censo 2010
***************************************************

g anoest = .

/* para quem frequenta escola */
replace anoest = 0 if v0628<=2 & v0629<=4						/* creche, pre, CA, alfabetizacao de adultos */
replace anoest = 0 if v0628<=2 & v0629==5 & v0630<=2			/* ensino fundamental seriado - 1a. serie */
replace anoest = v0630 - 2 if v0628<=2 & v0629==5 & v0630>=3 & v0630<=9	/* ensino fundamental seriado*/
replace anoest = 3 if v0628<=2 & v0629==5 & v0630==10 			/* ensino fundamental nao seriado */
replace anoest = 3 if v0628<=2 & v0629==6						/* EJA fundamental */

replace anoest = 8 if v0628<=2 & v0629==7 & (v0631==1 | v0631==5) 	/* ensino medio 1o. ano ou não seriado */
replace anoest = 9 if v0628<=2 & v0629==7 & v0631==2 				/* ensino medio 2o. ano */
replace anoest = 10 if v0628<=2 & v0629==7 & (v0631==3 | v0631==4)	/* ensino medio 3o. e 4o. anos */				
replace anoest = 8 if v0628<=2 & v0629==8							/* EJA medio */
replace anoest = 13 if v0628<=2 & v0629==9 & v0632==2				/* primeira graduaçao */
replace anoest = 15 if v0628<=2 & v0629==9 & v0632==1				/* ja cursou graduacao */

replace anoest = 15 if v0628<=2 & v0629>=10 & v0635==1		/* pos-graduacao com graduacao completa */
replace anoest = 16 if v0628<=2 & v0629>=10 & v0635>=2		/* pos-graduacao com pos-grad completa */

/* para quem nao frequenta */
replace anoest = 0 if v0628==3 & v0633<=2					/* creche, pre, CA, alfabetizacao de adultos */
replace anoest = 2 if v0628==3 & v0633==3 & v0634==2		/* antigo primario incompleto */
replace anoest = 4 if v0628==3 & v0633==3 & v0634==1		/* antigo primario completo */
replace anoest = 6 if v0628==3 & v0633==4 & v0634==2		/* ginasio incompleto */
replace anoest = 8 if v0628==3 & v0633==4 & v0634==1		/* ginasio completo */
replace anoest = 4 if v0628==3 & v0633==8 & v0634==2
replace anoest = 8 if v0628==3 & v0633==8 & v0634==1

replace anoest = 2 if v0628==3 & v0633==5					/* ensino fundamental incompleto - até 3a serie/4o. ano */
replace anoest = 4 if v0628==3 & v0633==6					/* ensino fundamental incompleto - 4a. serie/5o. ano */
replace anoest = 6 if v0628==3 & v0633==7 & v0634==2		/* ensino fundamental incompleto - até 7a.serie/8o. ano */
replace anoest = 8 if v0628==3 & v0633==7 & v0634==1		/* ensino fundamental completo */

replace anoest = 9 if v0628==3 & v0633==9 & v0634==2		/* antigo cientifico incompleto */
replace anoest = 11 if v0628==3 & v0633==9 & v0634==1		/* antigo cientifico completo */
replace anoest = 9 if v0628==3 & v0633==10 & v0634==2		/* ensino medio incompleto */
replace anoest = 11 if v0628==3 & v0633==10 & v0634==1		/* ensino medio completo */

replace anoest = 13 if v0628==3 & v0633==11 & v0634==2		/* superior incompleto */
replace anoest = 15 if v0628==3 & v0633==11 & v0634==1		/* superior completo */

replace anoest = 15 if v0628==3 & v0633>=12 & v0634==2		/* pos-grad incompleto */
replace anoest = 16 if v0628==3 & v0633>=12 & v0634==1		/* pos-grad completo */

/* para quem nunca frequentou */
replace anoest = 0 if v0628==4
