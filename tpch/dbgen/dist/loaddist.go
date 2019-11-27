package dist

type Item struct {
	Text   string
	Weight int32
}

type Dist []Item

var Maps = make(map[string]Dist)

var _ = func() error {
	Maps["category"] = []Item{
		{
			"FURNITURE", 1,
		},
		{
			"STORAGE EQUIP", 1,
		},
		{
			"TOOLS", 1,
		},
		{
			"MACHINE TOOLS", 1,
		},
		{
			"OTHER", 1,
		},
	}
	Maps["p_cntr"] = []Item{
		{
			"SM CASE", 1,
		},
		{
			"SM BOX", 1,
		},
		{
			"SM BAG", 1,
		},
		{
			"SM JAR", 1,
		},
		{
			"SM PACK", 1,
		},
		{
			"SM PKG", 1,
		},
		{
			"SM CAN", 1,
		},
		{
			"SM DRUM", 1,
		},
		{
			"LG CASE", 1,
		},
		{
			"LG BOX", 1,
		},
		{
			"LG BAG", 1,
		},
		{
			"LG JAR", 1,
		},
		{
			"LG PACK", 1,
		},
		{
			"LG PKG", 1,
		},
		{
			"LG CAN", 1,
		},
		{
			"LG DRUM", 1,
		},
		{
			"MED CASE", 1,
		},
		{
			"MED BOX", 1,
		},
		{
			"MED BAG", 1,
		},
		{
			"MED JAR", 1,
		},
		{
			"MED PACK", 1,
		},
		{
			"MED PKG", 1,
		},
		{
			"MED CAN", 1,
		},
		{
			"MED DRUM", 1,
		},
		{
			"JUMBO CASE", 1,
		},
		{
			"JUMBO BOX", 1,
		},
		{
			"JUMBO BAG", 1,
		},
		{
			"JUMBO JAR", 1,
		},
		{
			"JUMBO PACK", 1,
		},
		{
			"JUMBO PKG", 1,
		},
		{
			"JUMBO CAN", 1,
		},
		{
			"JUMBO DRUM", 1,
		},
		{
			"WRAP CASE", 1,
		},
		{
			"WRAP BOX", 1,
		},
		{
			"WRAP BAG", 1,
		},
		{
			"WRAP JAR", 1,
		},
		{
			"WRAP PACK", 1,
		},
		{
			"WRAP PKG", 1,
		},
		{
			"WRAP CAN", 1,
		},
		{
			"WRAP DRUM", 1,
		},
	}
	Maps["instruct"] = []Item{
		{
			"DELIVER IN PERSON", 1,
		},
		{
			"COLLECT COD", 1,
		},
		{
			"TAKE BACK RETURN", 1,
		},
		{
			"NONE", 1,
		},
	}
	Maps["msegmnt"] = []Item{
		{
			"AUTOMOBILE", 1,
		},
		{
			"BUILDING", 1,
		},
		{
			"FURNITURE", 1,
		},
		{
			"HOUSEHOLD", 1,
		},
		{
			"MACHINERY", 1,
		},
	}
	Maps["p_names"] = []Item{
		{
			"CLEANER", 1,
		},
		{
			"SOAP", 1,
		},
		{
			"DETERGENT", 1,
		},
		{
			"EXTRA", 1,
		},
	}
	Maps["nations"] = []Item{
		{
			"ALGERIA", 0,
		},
		{
			"ARGENTINA", 1,
		},
		{
			"BRAZIL", 0,
		},
		{
			"CANADA", 0,
		},
		{
			"EGYPT", 3,
		},
		{
			"ETHIOPIA", -4,
		},
		{
			"FRANCE", 3,
		},
		{
			"GERMANY", 0,
		},
		{
			"INDIA", -1,
		},
		{
			"INDONESIA", 0,
		},
		{
			"IRAN", 2,
		},
		{
			"IRAQ", 0,
		},
		{
			"JAPAN", -2,
		},
		{
			"JORDAN", 2,
		},
		{
			"KENYA", -4,
		},
		{
			"MOROCCO", 0,
		},
		{
			"MOZAMBIQUE", 0,
		},
		{
			"PERU", 1,
		},
		{
			"CHINA", 1,
		},
		{
			"ROMANIA", 1,
		},
		{
			"SAUDI ARABIA", 1,
		},
		{
			"VIETNAM", -2,
		},
		{
			"RUSSIA", 1,
		},
		{
			"UNITED KINGDOM", 0,
		},
		{
			"UNITED STATES", -2,
		},
	}
	Maps["nations2"] = []Item{
		{
			"ALGERIA", 1,
		},
		{
			"ARGENTINA", 1,
		},
		{
			"BRAZIL", 1,
		},
		{
			"CANADA", 1,
		},
		{
			"EGYPT", 1,
		},
		{
			"ETHIOPIA", 1,
		},
		{
			"FRANCE", 1,
		},
		{
			"GERMANY", 1,
		},
		{
			"INDIA", 1,
		},
		{
			"INDONESIA", 1,
		},
		{
			"IRAN", 1,
		},
		{
			"IRAQ", 1,
		},
		{
			"JAPAN", 1,
		},
		{
			"JORDAN", 1,
		},
		{
			"KENYA", 1,
		},
		{
			"MOROCCO", 1,
		},
		{
			"MOZAMBIQUE", 1,
		},
		{
			"PERU", 1,
		},
		{
			"CHINA", 1,
		},
		{
			"ROMANIA", 1,
		},
		{
			"SAUDI ARABIA", 1,
		},
		{
			"VIETNAM", 1,
		},
		{
			"RUSSIA", 1,
		},
		{
			"UNITED KINGDOM", 1,
		},
		{
			"UNITED STATES", 1,
		},
	}
	Maps["regions"] = []Item{
		{
			"AFRICA", 1,
		},
		{
			"AMERICA", 1,
		},
		{
			"ASIA", 1,
		},
		{
			"EUROPE", 1,
		},
		{
			"MIDDLE EAST", 1,
		},
	}
	Maps["o_oprio"] = []Item{
		{
			"1-URGENT", 1,
		},
		{
			"2-HIGH", 1,
		},
		{
			"3-MEDIUM", 1,
		},
		{
			"4-NOT SPECIFIED", 1,
		},
		{
			"5-LOW", 1,
		},
	}
	Maps["rflag"] = []Item{
		{
			"R", 1,
		},
		{
			"A", 1,
		},
	}
	Maps["smode"] = []Item{
		{
			"REG AIR", 1,
		},
		{
			"AIR", 1,
		},
		{
			"RAIL", 1,
		},
		{
			"TRUCK", 1,
		},
		{
			"MAIL", 1,
		},
		{
			"FOB", 1,
		},
		{
			"SHIP", 1,
		},
	}
	Maps["p_types"] = []Item{
		{
			"STANDARD ANODIZED TIN", 1,
		},
		{
			"STANDARD ANODIZED NICKEL", 1,
		},
		{
			"STANDARD ANODIZED BRASS", 1,
		},
		{
			"STANDARD ANODIZED STEEL", 1,
		},
		{
			"STANDARD ANODIZED COPPER", 1,
		},
		{
			"STANDARD BURNISHED TIN", 1,
		},
		{
			"STANDARD BURNISHED NICKEL", 1,
		},
		{
			"STANDARD BURNISHED BRASS", 1,
		},
		{
			"STANDARD BURNISHED STEEL", 1,
		},
		{
			"STANDARD BURNISHED COPPER", 1,
		},
		{
			"STANDARD PLATED TIN", 1,
		},
		{
			"STANDARD PLATED NICKEL", 1,
		},
		{
			"STANDARD PLATED BRASS", 1,
		},
		{
			"STANDARD PLATED STEEL", 1,
		},
		{
			"STANDARD PLATED COPPER", 1,
		},
		{
			"STANDARD POLISHED TIN", 1,
		},
		{
			"STANDARD POLISHED NICKEL", 1,
		},
		{
			"STANDARD POLISHED BRASS", 1,
		},
		{
			"STANDARD POLISHED STEEL", 1,
		},
		{
			"STANDARD POLISHED COPPER", 1,
		},
		{
			"STANDARD BRUSHED TIN", 1,
		},
		{
			"STANDARD BRUSHED NICKEL", 1,
		},
		{
			"STANDARD BRUSHED BRASS", 1,
		},
		{
			"STANDARD BRUSHED STEEL", 1,
		},
		{
			"STANDARD BRUSHED COPPER", 1,
		},
		{
			"SMALL ANODIZED TIN", 1,
		},
		{
			"SMALL ANODIZED NICKEL", 1,
		},
		{
			"SMALL ANODIZED BRASS", 1,
		},
		{
			"SMALL ANODIZED STEEL", 1,
		},
		{
			"SMALL ANODIZED COPPER", 1,
		},
		{
			"SMALL BURNISHED TIN", 1,
		},
		{
			"SMALL BURNISHED NICKEL", 1,
		},
		{
			"SMALL BURNISHED BRASS", 1,
		},
		{
			"SMALL BURNISHED STEEL", 1,
		},
		{
			"SMALL BURNISHED COPPER", 1,
		},
		{
			"SMALL PLATED TIN", 1,
		},
		{
			"SMALL PLATED NICKEL", 1,
		},
		{
			"SMALL PLATED BRASS", 1,
		},
		{
			"SMALL PLATED STEEL", 1,
		},
		{
			"SMALL PLATED COPPER", 1,
		},
		{
			"SMALL POLISHED TIN", 1,
		},
		{
			"SMALL POLISHED NICKEL", 1,
		},
		{
			"SMALL POLISHED BRASS", 1,
		},
		{
			"SMALL POLISHED STEEL", 1,
		},
		{
			"SMALL POLISHED COPPER", 1,
		},
		{
			"SMALL BRUSHED TIN", 1,
		},
		{
			"SMALL BRUSHED NICKEL", 1,
		},
		{
			"SMALL BRUSHED BRASS", 1,
		},
		{
			"SMALL BRUSHED STEEL", 1,
		},
		{
			"SMALL BRUSHED COPPER", 1,
		},
		{
			"MEDIUM ANODIZED TIN", 1,
		},
		{
			"MEDIUM ANODIZED NICKEL", 1,
		},
		{
			"MEDIUM ANODIZED BRASS", 1,
		},
		{
			"MEDIUM ANODIZED STEEL", 1,
		},
		{
			"MEDIUM ANODIZED COPPER", 1,
		},
		{
			"MEDIUM BURNISHED TIN", 1,
		},
		{
			"MEDIUM BURNISHED NICKEL", 1,
		},
		{
			"MEDIUM BURNISHED BRASS", 1,
		},
		{
			"MEDIUM BURNISHED STEEL", 1,
		},
		{
			"MEDIUM BURNISHED COPPER", 1,
		},
		{
			"MEDIUM PLATED TIN", 1,
		},
		{
			"MEDIUM PLATED NICKEL", 1,
		},
		{
			"MEDIUM PLATED BRASS", 1,
		},
		{
			"MEDIUM PLATED STEEL", 1,
		},
		{
			"MEDIUM PLATED COPPER", 1,
		},
		{
			"MEDIUM POLISHED TIN", 1,
		},
		{
			"MEDIUM POLISHED NICKEL", 1,
		},
		{
			"MEDIUM POLISHED BRASS", 1,
		},
		{
			"MEDIUM POLISHED STEEL", 1,
		},
		{
			"MEDIUM POLISHED COPPER", 1,
		},
		{
			"MEDIUM BRUSHED TIN", 1,
		},
		{
			"MEDIUM BRUSHED NICKEL", 1,
		},
		{
			"MEDIUM BRUSHED BRASS", 1,
		},
		{
			"MEDIUM BRUSHED STEEL", 1,
		},
		{
			"MEDIUM BRUSHED COPPER", 1,
		},
		{
			"LARGE ANODIZED TIN", 1,
		},
		{
			"LARGE ANODIZED NICKEL", 1,
		},
		{
			"LARGE ANODIZED BRASS", 1,
		},
		{
			"LARGE ANODIZED STEEL", 1,
		},
		{
			"LARGE ANODIZED COPPER", 1,
		},
		{
			"LARGE BURNISHED TIN", 1,
		},
		{
			"LARGE BURNISHED NICKEL", 1,
		},
		{
			"LARGE BURNISHED BRASS", 1,
		},
		{
			"LARGE BURNISHED STEEL", 1,
		},
		{
			"LARGE BURNISHED COPPER", 1,
		},
		{
			"LARGE PLATED TIN", 1,
		},
		{
			"LARGE PLATED NICKEL", 1,
		},
		{
			"LARGE PLATED BRASS", 1,
		},
		{
			"LARGE PLATED STEEL", 1,
		},
		{
			"LARGE PLATED COPPER", 1,
		},
		{
			"LARGE POLISHED TIN", 1,
		},
		{
			"LARGE POLISHED NICKEL", 1,
		},
		{
			"LARGE POLISHED BRASS", 1,
		},
		{
			"LARGE POLISHED STEEL", 1,
		},
		{
			"LARGE POLISHED COPPER", 1,
		},
		{
			"LARGE BRUSHED TIN", 1,
		},
		{
			"LARGE BRUSHED NICKEL", 1,
		},
		{
			"LARGE BRUSHED BRASS", 1,
		},
		{
			"LARGE BRUSHED STEEL", 1,
		},
		{
			"LARGE BRUSHED COPPER", 1,
		},
		{
			"ECONOMY ANODIZED TIN", 1,
		},
		{
			"ECONOMY ANODIZED NICKEL", 1,
		},
		{
			"ECONOMY ANODIZED BRASS", 1,
		},
		{
			"ECONOMY ANODIZED STEEL", 1,
		},
		{
			"ECONOMY ANODIZED COPPER", 1,
		},
		{
			"ECONOMY BURNISHED TIN", 1,
		},
		{
			"ECONOMY BURNISHED NICKEL", 1,
		},
		{
			"ECONOMY BURNISHED BRASS", 1,
		},
		{
			"ECONOMY BURNISHED STEEL", 1,
		},
		{
			"ECONOMY BURNISHED COPPER", 1,
		},
		{
			"ECONOMY PLATED TIN", 1,
		},
		{
			"ECONOMY PLATED NICKEL", 1,
		},
		{
			"ECONOMY PLATED BRASS", 1,
		},
		{
			"ECONOMY PLATED STEEL", 1,
		},
		{
			"ECONOMY PLATED COPPER", 1,
		},
		{
			"ECONOMY POLISHED TIN", 1,
		},
		{
			"ECONOMY POLISHED NICKEL", 1,
		},
		{
			"ECONOMY POLISHED BRASS", 1,
		},
		{
			"ECONOMY POLISHED STEEL", 1,
		},
		{
			"ECONOMY POLISHED COPPER", 1,
		},
		{
			"ECONOMY BRUSHED TIN", 1,
		},
		{
			"ECONOMY BRUSHED NICKEL", 1,
		},
		{
			"ECONOMY BRUSHED BRASS", 1,
		},
		{
			"ECONOMY BRUSHED STEEL", 1,
		},
		{
			"ECONOMY BRUSHED COPPER", 1,
		},
		{
			"PROMO ANODIZED TIN", 1,
		},
		{
			"PROMO ANODIZED NICKEL", 1,
		},
		{
			"PROMO ANODIZED BRASS", 1,
		},
		{
			"PROMO ANODIZED STEEL", 1,
		},
		{
			"PROMO ANODIZED COPPER", 1,
		},
		{
			"PROMO BURNISHED TIN", 1,
		},
		{
			"PROMO BURNISHED NICKEL", 1,
		},
		{
			"PROMO BURNISHED BRASS", 1,
		},
		{
			"PROMO BURNISHED STEEL", 1,
		},
		{
			"PROMO BURNISHED COPPER", 1,
		},
		{
			"PROMO PLATED TIN", 1,
		},
		{
			"PROMO PLATED NICKEL", 1,
		},
		{
			"PROMO PLATED BRASS", 1,
		},
		{
			"PROMO PLATED STEEL", 1,
		},
		{
			"PROMO PLATED COPPER", 1,
		},
		{
			"PROMO POLISHED TIN", 1,
		},
		{
			"PROMO POLISHED NICKEL", 1,
		},
		{
			"PROMO POLISHED BRASS", 1,
		},
		{
			"PROMO POLISHED STEEL", 1,
		},
		{
			"PROMO POLISHED COPPER", 1,
		},
		{
			"PROMO BRUSHED TIN", 1,
		},
		{
			"PROMO BRUSHED NICKEL", 1,
		},
		{
			"PROMO BRUSHED BRASS", 1,
		},
		{
			"PROMO BRUSHED STEEL", 1,
		},
		{
			"PROMO BRUSHED COPPER", 1,
		},
	}
	Maps["colors"] = []Item{
		{
			"almond", 1,
		},
		{
			"antique", 1,
		},
		{
			"aquamarine", 1,
		},
		{
			"azure", 1,
		},
		{
			"beige", 1,
		},
		{
			"bisque", 1,
		},
		{
			"black", 1,
		},
		{
			"blanched", 1,
		},
		{
			"blue", 1,
		},
		{
			"blush", 1,
		},
		{
			"brown", 1,
		},
		{
			"burlywood", 1,
		},
		{
			"burnished", 1,
		},
		{
			"chartreuse", 1,
		},
		{
			"chiffon", 1,
		},
		{
			"chocolate", 1,
		},
		{
			"coral", 1,
		},
		{
			"cornflower", 1,
		},
		{
			"cornsilk", 1,
		},
		{
			"cream", 1,
		},
		{
			"cyan", 1,
		},
		{
			"dark", 1,
		},
		{
			"deep", 1,
		},
		{
			"dim", 1,
		},
		{
			"dodger", 1,
		},
		{
			"drab", 1,
		},
		{
			"firebrick", 1,
		},
		{
			"floral", 1,
		},
		{
			"forest", 1,
		},
		{
			"frosted", 1,
		},
		{
			"gainsboro", 1,
		},
		{
			"ghost", 1,
		},
		{
			"goldenrod", 1,
		},
		{
			"green", 1,
		},
		{
			"grey", 1,
		},
		{
			"honeydew", 1,
		},
		{
			"hot", 1,
		},
		{
			"indian", 1,
		},
		{
			"ivory", 1,
		},
		{
			"khaki", 1,
		},
		{
			"lace", 1,
		},
		{
			"lavender", 1,
		},
		{
			"lawn", 1,
		},
		{
			"lemon", 1,
		},
		{
			"light", 1,
		},
		{
			"lime", 1,
		},
		{
			"linen", 1,
		},
		{
			"magenta", 1,
		},
		{
			"maroon", 1,
		},
		{
			"medium", 1,
		},
		{
			"metallic", 1,
		},
		{
			"midnight", 1,
		},
		{
			"mint", 1,
		},
		{
			"misty", 1,
		},
		{
			"moccasin", 1,
		},
		{
			"navajo", 1,
		},
		{
			"navy", 1,
		},
		{
			"olive", 1,
		},
		{
			"orange", 1,
		},
		{
			"orchid", 1,
		},
		{
			"pale", 1,
		},
		{
			"papaya", 1,
		},
		{
			"peach", 1,
		},
		{
			"peru", 1,
		},
		{
			"pink", 1,
		},
		{
			"plum", 1,
		},
		{
			"powder", 1,
		},
		{
			"puff", 1,
		},
		{
			"purple", 1,
		},
		{
			"red", 1,
		},
		{
			"rose", 1,
		},
		{
			"rosy", 1,
		},
		{
			"royal", 1,
		},
		{
			"saddle", 1,
		},
		{
			"salmon", 1,
		},
		{
			"sandy", 1,
		},
		{
			"seashell", 1,
		},
		{
			"sienna", 1,
		},
		{
			"sky", 1,
		},
		{
			"slate", 1,
		},
		{
			"smoke", 1,
		},
		{
			"snow", 1,
		},
		{
			"spring", 1,
		},
		{
			"steel", 1,
		},
		{
			"tan", 1,
		},
		{
			"thistle", 1,
		},
		{
			"tomato", 1,
		},
		{
			"turquoise", 1,
		},
		{
			"violet", 1,
		},
		{
			"wheat", 1,
		},
		{
			"white", 1,
		},
		{
			"yellow", 1,
		},
	}
	Maps["nouns"] = []Item{
		{
			"packages", 40,
		},
		{
			"requests", 40,
		},
		{
			"accounts", 40,
		},
		{
			"deposits", 40,
		},
		{
			"foxes", 20,
		},
		{
			"ideas", 20,
		},
		{
			"theodolites", 20,
		},
		{
			"pinto beans", 20,
		},
		{
			"instructions", 20,
		},
		{
			"dependencies", 10,
		},
		{
			"excuses", 10,
		},
		{
			"platelets", 10,
		},
		{
			"asymptotes", 10,
		},
		{
			"courts", 5,
		},
		{
			"dolphins", 5,
		},
		{
			"multipliers", 1,
		},
		{
			"sauternes", 1,
		},
		{
			"warthogs", 1,
		},
		{
			"frets", 1,
		},
		{
			"dinos", 1,
		},
		{
			"attainments", 1,
		},
		{
			"somas", 1,
		},
		{
			"Tiresias", 1,
		},
		{
			"patterns", 1,
		},
		{
			"forges", 1,
		},
		{
			"braids", 1,
		},
		{
			"frays", 1,
		},
		{
			"warhorses", 1,
		},
		{
			"dugouts", 1,
		},
		{
			"notornis", 1,
		},
		{
			"epitaphs", 1,
		},
		{
			"pearls", 1,
		},
		{
			"tithes", 1,
		},
		{
			"waters", 1,
		},
		{
			"orbits", 1,
		},
		{
			"gifts", 1,
		},
		{
			"sheaves", 1,
		},
		{
			"depths", 1,
		},
		{
			"sentiments", 1,
		},
		{
			"decoys", 1,
		},
		{
			"realms", 1,
		},
		{
			"pains", 1,
		},
		{
			"grouches", 1,
		},
		{
			"escapades", 1,
		},
		{
			"hockey players", 1,
		},
	}
	Maps["verbs"] = []Item{
		{
			"sleep", 20,
		},
		{
			"wake", 20,
		},
		{
			"are", 20,
		},
		{
			"cajole", 20,
		},
		{
			"haggle", 20,
		},
		{
			"nag", 10,
		},
		{
			"use", 10,
		},
		{
			"boost", 10,
		},
		{
			"affix", 5,
		},
		{
			"detect", 5,
		},
		{
			"integrate", 5,
		},
		{
			"maintain", 1,
		},
		{
			"nod", 1,
		},
		{
			"was", 1,
		},
		{
			"lose", 1,
		},
		{
			"sublate", 1,
		},
		{
			"solve", 1,
		},
		{
			"thrash", 1,
		},
		{
			"promise", 1,
		},
		{
			"engage", 1,
		},
		{
			"hinder", 1,
		},
		{
			"print", 1,
		},
		{
			"x-ray", 1,
		},
		{
			"breach", 1,
		},
		{
			"eat", 1,
		},
		{
			"grow", 1,
		},
		{
			"impress", 1,
		},
		{
			"mold", 1,
		},
		{
			"poach", 1,
		},
		{
			"serve", 1,
		},
		{
			"run", 1,
		},
		{
			"dazzle", 1,
		},
		{
			"snooze", 1,
		},
		{
			"doze", 1,
		},
		{
			"unwind", 1,
		},
		{
			"kindle", 1,
		},
		{
			"play", 1,
		},
		{
			"hang", 1,
		},
		{
			"believe", 1,
		},
		{
			"doubt", 1,
		},
	}
	Maps["adverbs"] = []Item{
		{
			"sometimes", 1,
		},
		{
			"always", 1,
		},
		{
			"never", 1,
		},
		{
			"furiously", 50,
		},
		{
			"slyly", 50,
		},
		{
			"carefully", 50,
		},
		{
			"blithely", 40,
		},
		{
			"quickly", 30,
		},
		{
			"fluffily", 20,
		},
		{
			"slowly", 1,
		},
		{
			"quietly", 1,
		},
		{
			"ruthlessly", 1,
		},
		{
			"thinly", 1,
		},
		{
			"closely", 1,
		},
		{
			"doggedly", 1,
		},
		{
			"daringly", 1,
		},
		{
			"bravely", 1,
		},
		{
			"stealthily", 1,
		},
		{
			"permanently", 1,
		},
		{
			"enticingly", 1,
		},
		{
			"idly", 1,
		},
		{
			"busily", 1,
		},
		{
			"regularly", 1,
		},
		{
			"finally", 1,
		},
		{
			"ironically", 1,
		},
		{
			"evenly", 1,
		},
		{
			"boldly", 1,
		},
		{
			"silently", 1,
		},
	}
	Maps["articles"] = []Item{
		{
			"the", 50,
		},
		{
			"a", 20,
		},
		{
			"an", 5,
		},
	}
	Maps["prepositions"] = []Item{
		{
			"about", 50,
		},
		{
			"above", 50,
		},
		{
			"according to", 50,
		},
		{
			"across", 50,
		},
		{
			"after", 50,
		},
		{
			"against", 40,
		},
		{
			"along", 40,
		},
		{
			"alongside of", 30,
		},
		{
			"among", 30,
		},
		{
			"around", 20,
		},
		{
			"at", 10,
		},
		{
			"atop", 1,
		},
		{
			"before", 1,
		},
		{
			"behind", 1,
		},
		{
			"beneath", 1,
		},
		{
			"beside", 1,
		},
		{
			"besides", 1,
		},
		{
			"between", 1,
		},
		{
			"beyond", 1,
		},
		{
			"by", 1,
		},
		{
			"despite", 1,
		},
		{
			"during", 1,
		},
		{
			"except", 1,
		},
		{
			"for", 1,
		},
		{
			"from", 1,
		},
		{
			"in place of", 1,
		},
		{
			"inside", 1,
		},
		{
			"instead of", 1,
		},
		{
			"into", 1,
		},
		{
			"near", 1,
		},
		{
			"of", 1,
		},
		{
			"on", 1,
		},
		{
			"outside", 1,
		},
		{
			"over", 1,
		},
		{
			"past", 1,
		},
		{
			"since", 1,
		},
		{
			"through", 1,
		},
		{
			"throughout", 1,
		},
		{
			"to", 1,
		},
		{
			"toward", 1,
		},
		{
			"under", 1,
		},
		{
			"until", 1,
		},
		{
			"up", 1,
		},
		{
			"upon", 1,
		},
		{
			"whithout", 1,
		},
		{
			"with", 1,
		},
		{
			"within", 1,
		},
	}
	Maps["auxillaries"] = []Item{
		{
			"do", 1,
		},
		{
			"may", 1,
		},
		{
			"might", 1,
		},
		{
			"shall", 1,
		},
		{
			"will", 1,
		},
		{
			"would", 1,
		},
		{
			"can", 1,
		},
		{
			"could", 1,
		},
		{
			"should", 1,
		},
		{
			"ought to", 1,
		},
		{
			"must", 1,
		},
		{
			"will have to", 1,
		},
		{
			"shall have to", 1,
		},
		{
			"could have to", 1,
		},
		{
			"should have to", 1,
		},
		{
			"must have to", 1,
		},
		{
			"need to", 1,
		},
		{
			"try to", 1,
		},
	}
	Maps["terminators"] = []Item{
		{
			".", 50,
		},
		{
			";", 1,
		},
		{
			":", 1,
		},
		{
			"?", 1,
		},
		{
			"!", 1,
		},
		{
			"--", 1,
		},
	}
	Maps["adjectives"] = []Item{
		{
			"special", 20,
		},
		{
			"pending", 20,
		},
		{
			"unusual", 20,
		},
		{
			"express", 20,
		},
		{
			"furious", 1,
		},
		{
			"sly", 1,
		},
		{
			"careful", 1,
		},
		{
			"blithe", 1,
		},
		{
			"quick", 1,
		},
		{
			"fluffy", 1,
		},
		{
			"slow", 1,
		},
		{
			"quiet", 1,
		},
		{
			"ruthless", 1,
		},
		{
			"thin", 1,
		},
		{
			"close", 1,
		},
		{
			"dogged", 1,
		},
		{
			"daring", 1,
		},
		{
			"brave", 1,
		},
		{
			"stealthy", 1,
		},
		{
			"permanent", 1,
		},
		{
			"enticing", 1,
		},
		{
			"idle", 1,
		},
		{
			"busy", 1,
		},
		{
			"regular", 50,
		},
		{
			"final", 40,
		},
		{
			"ironic", 40,
		},
		{
			"even", 30,
		},
		{
			"bold", 20,
		},
		{
			"silent", 10,
		},
	}
	Maps["grammar"] = []Item{
		{
			"N V T", 3,
		},
		{
			"N V P T", 3,
		},
		{
			"N V N T", 3,
		},
		{
			"N P V N T", 1,
		},
		{
			"N P V P T", 1,
		},
	}
	Maps["np"] = []Item{
		{
			"N", 10,
		},
		{
			"J N", 20,
		},
		{
			"J, J N", 10,
		},
		{
			"D J N", 50,
		},
	}
	Maps["vp"] = []Item{
		{
			"V", 30,
		},
		{
			"X V", 1,
		},
		{
			"V D", 40,
		},
		{
			"X V D", 1,
		},
	}
	Maps["Q13a"] = []Item{
		{
			"special", 20,
		},
		{
			"pending", 20,
		},
		{
			"unusual", 20,
		},
		{
			"express", 20,
		},
	}
	Maps["Q13b"] = []Item{
		{
			"packages", 40,
		},
		{
			"requests", 40,
		},
		{
			"accounts", 40,
		},
		{
			"deposits", 40,
		},
	}
	return nil
}()
