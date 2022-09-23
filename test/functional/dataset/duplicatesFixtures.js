module.exports.duplicatesFixtures = [
  {
    business: {
      duplicates: [
        {
          sessionName: 'TEST_SESSION',
          source: 'crossref',
          sourceUid: 'crossref$10.1001/jama.2014.10498',
          rules: ['RULE_66'],
        },
      ],
      name: 'corhal',
      duplicateGenre: 'Article',
      xissn: '0098-7484',
      xPublicationDate: '2015-01-20',
      first3AuthorNames: 'Marret Stéphane Bénichou Jacques',
      first3AuthorNamesWithInitials: 'Marret S Bénichou J',
      pageRange: '306',
      hasDoi: true,
      hasFulltext: false,
      sourceUidChain: '',
    },
    technical: {
      sessionName: 'ANOTHER_SESSION',
      creationDate: '2021-02-19 10:02:12',
      internalId: 'b$6',
    },
    language: ['English'],
    doi: '10.1001/jama.2014.15912',
    originalGenre: 'Journal article',
    title: {
      default: 'Perioperative Aspirin and Clonidine and Risk of Acute Kidney Injury',
      en: 'Perioperative Aspirin and Clonidine and Risk of Acute Kidney Injury',
    },
    host: {
      title: 'JAMA',
      eisbn: '978-3-319-78926-2',
      isbn: '978-3-319-78924-8',
      eissn: '2380-6591',
      issn: '0098-7484',
      electronicPublicationDate: '2017-10-23',
      publicationDate: '2015-01-20',
      issue: '3',
      language: ['English'],
      part: '10 Pt A',
      specialIssue: 'P1',
      supplement: 'Suppl 1',
      publisher: 'American Medical Association ',
      pages: [{ range: '306-310', total: 0 }, { range: '666' }],
      volume: '313',
    },
    source: 'B',
    sourceUid: 'b$6',
  },
  {
    business: {
      name: 'corhal',
      duplicateGenre: 'Article',
      xissn: '0098-7484',
      xPublicationDate: '2014-09-24',
      first3AuthorNames: 'Herrero Rolando Parsonnet Julie Greenberg Edwin Robert',
      first3AuthorNamesWithInitials: 'Herrero R Parsonnet J Greenberg ER',
      pageRange: '1197',
      hasDoi: true,
      hasFulltext: false,
      sourceUidChain: '!crossref$10.1001/jama.2014.10498!',
      sources: ['crossref'],
    },
    technical: {
      internalId: 'crossref$10.1001/jama.2014.10498',
    },
    language: ['English'],
    title: { default: 'Prevention of Gastric Cancer', en: 'Prevention of Gastric Cancer' },
    authors: [
      {
        forename: 'Rolando',
        surname: 'Herrero',
        affiliations: [
          {
            address: 'Section of Early Detection and Prevention, International Agency for Research on Cancer, Lyon, France',
          },
        ],
        fullname: 'Herrero Rolando',
      },
      {
        forename: 'Julie',
        surname: 'Parsonnet',
        affiliations: [
          { address: 'Department of Medicine, Stanford University Medical Center, Stanford, California' },
        ],
        fullname: 'Parsonnet Julie',
      },
      {
        forename: 'Edwin Robert',
        surname: 'Greenberg',
        affiliations: [
          {
            address: 'Division of Public Health Sciences, Fred Hutchinson Cancer Research Center, Seattle, Washington',
          },
        ],
        fullname: 'Greenberg Edwin Robert',
      },
    ],
    doi: '10.1001/jama.2014.10498',
    originalGenre: 'Journal article',
    host: {
      title: 'JAMA',
      eisbn: '978-1-5386-6916-7',
      isbn: '9782759224081',
      eissn: '1538-3598',
      issn: '0098-7484',
      electronicPublicationDate: '2016-07-19',
      publicationDate: '2014-09-24',
      issue: '12',
      language: ['English'],
      part: 'Pt 2',
      specialIssue: 'C',
      supplement: 'Suppl 2',
      publisher: 'American Chemical Society',
      pages: [{ range: '1197', total: 0 }],
      volume: '312',
    },
    source: 'crossref',
    sourceId: '10.1001/jama.2014.10498',
    sourceUid: 'crossref$10.1001/jama.2014.10498',
  },
  {
    business: {
      name: 'corhal',
      duplicateGenre: 'Article',
      xissn: '1538-3598',
      xPublicationDate: '2014-09-24',
      first3AuthorNames: 'Herrero Rolando Parsonnet Julie Greenberg Edwin Robert',
      first3AuthorNamesWithInitials: 'Herrero R Parsonnet J Greenberg ER',
      pageRange: '1197-8',
      hasDoi: true,
      hasFulltext: false,
      sourceUidChain: '!pubmed$25247512!',
      sources: ['pubmed'],
    },
    technical: {
      internalId: 'pubmed$25247512',
    },
    language: ['English'],
    keywords: {
      en: {
        author: ['iron(II) complexes', 'molecular magnetism', 'polymorphism', 'spin crossover'],
        mesh: [
          'Health Policy',
          'Helicobacter Infections',
          'diagnosis',
          'drug therapy',
          'Humans',
          'Randomized Controlled Trials as Topic',
          'Stomach Neoplasms',
          'microbiology',
          'prevention & control',
          'United States',
        ],
      },
      fr: { author: ['conceptual graphs', 'inconsistencies', 'conceptual graphs', 'management', 'domain'] },
    },
    title: { default: 'Prevention of gastric cancer.', en: 'Prevention of gastric cancer.' },
    authors: [
      {
        forename: 'Rolando',
        surname: 'Herrero',
        affiliations: [
          {
            address: 'Section of Early Detection and Prevention, International Agency for Research on Cancer, Lyon, France.',
          },
        ],
        fullname: 'Herrero Rolando',
      },
      {
        forename: 'Julie',
        surname: 'Parsonnet',
        affiliations: [
          { address: 'Department of Medicine, Stanford University Medical Center, Stanford, California.' },
        ],
        fullname: 'Parsonnet Julie',
      },
      {
        forename: 'Edwin Robert',
        surname: 'Greenberg',
        affiliations: [
          {
            address: 'Division of Public Health Sciences, Fred Hutchinson Cancer Research Center, Seattle, Washington.',
          },
        ],
        fullname: 'Greenberg Edwin Robert',
      },
    ],
    pii: '1906623',
    doi: '10.1001/jama.2014.10498',
    pmId: '25247512',
    originalGenre: 'Journal Article',
    host: {
      title: 'JAMA',
      eisbn: '978-1-5386-6916-7',
      isbn: '9782759224081',
      eissn: '1538-3598',
      issn: '1996-1944',
      electronicPublicationDate: '2016-07-19',
      publicationDate: '2014-09-24',
      issue: '12',
      language: ['English'],
      part: 'Pt 2',
      specialIssue: 'C',
      supplement: 'Suppl 2',
      publisher: 'American Chemical Society',
      pages: [{ range: '1197-8', total: 0 }],
      volume: '312',
    },
    source: 'pubmed',
    sourceId: '25247512',
    sourceUid: 'pubmed$25247512',
  },
  {
    business: {
      name: 'corhal',
      duplicateGenre: 'Article',
      duplicates: [
        {
          sourceUid: 'crossref$10.1001/jama.2014.15912',
          internalId: 'QVtJr9XOWFjIcbXWTLSXfZGN5',
          sessionName: 'TEST_SESSION',
          source: 'crossref',
          rules: ['RULE_111'],
        },
        {
          // duplicate from the current session are kept
          sourceUid: 'x$1',
          sessionName: 'TEST_SESSION',
          source: 'x',
          rules: ['RULE_111'],
        },
      ],
      xissn: '1538-3598',
      xPublicationDate: '2015-01-20',
      first3AuthorNames: 'Marret Stéphane Bénichou Jacques',
      first3AuthorNamesWithInitials: 'Marret S Bénichou J',
      pageRange: '306',
      hasDoi: true,
      hasFulltext: false,
      sourceUidChain: '!pubmed$25603006!',
    },
    technical: {
      sessionName: 'PUBMED_2020-12-18_2015',
      creationDate: '2020-12-18 14:53:30',
      internalId: 'sr5Dmc0w0gnPDRoHJQbaUD1yy',
    },
    language: ['English'],
    keywords: {
      en: {
        author: [
          'WEIGHT',
          'body-mass index',
          'waist-hip ratio',
          'sleep duration',
          'sociodemographic factors',
          'risk-factors',
          'obesity',
          'adults',
          'circumference',
          'weight',
        ],
        mesh: [
          'Cerebral Palsy',
          'prevention & control',
          'Cognition Disorders',
          'prevention & control',
          'Female',
          'Humans',
          'Infant, Extremely Premature',
          'Magnesium Sulfate',
          'therapeutic use',
          'Male',
          'Motor Skills Disorders',
          'prevention & control',
          'Neuroprotective Agents',
          'therapeutic use',
          'Pregnancy',
          'Neuroprotective Agents',
          'Magnesium Sulfate',
        ],
      },
      fr: { author: ['general-population'] },
    },
    title: {
      default: 'Antenatal magnesium sulfate and outcomes for school-aged children.',
      en: 'Antenatal magnesium sulfate and outcomes for school-aged children.',
    },
    authors: [
      {
        forename: 'Stéphane',
        surname: 'Marret',
        affiliations: [
          {
            address: 'Department of Neonatal Medicine, Rouen University Hospital and Région-INSERM (ERI 28), Normandy University, Rouen, France.',
          },
        ],
        fullname: 'Marret Stéphane',
      },
      {
        forename: 'Jacques',
        surname: 'Bénichou',
        affiliations: [
          { address: 'Department of Biostatistics and INSERM UMR 657, Normandy University, Rouen, France.' },
        ],
        fullname: 'Bénichou Jacques',
      },
    ],
    pii: '2091297',
    doi: '10.1001/jama.2014.15912',
    pmId: '25603006',
    originalGenre: 'Letter,Comment',
    host: {
      title: 'JAMA',
      eisbn: '978-3-319-78926-2',
      isbn: '978-3-319-78924-8',
      eissn: '1538-3598',
      issn: '1101-1262',
      electronicPublicationDate: '2017-10-23',
      publicationDate: '2015-01-20',
      issue: '3',
      language: ['English'],
      part: '10 Pt A',
      specialIssue: 'P1',
      supplement: 'Suppl 1',
      publisher: 'Oxford University Press (OUP): Policy B - Oxford Open Option D',
      pages: [{ range: '306', total: 0 }],
      volume: '313',
    },
    source: 'pubmed',
    sourceUid: 'pubmed$25603006',
  },
  {
    business: {
      name: 'corhal',
      duplicateGenre: 'Article',
      duplicates: [
        {
          sessionName: 'ANOTHER_SESSION',
          source: 'A',
          sourceUid: 'A$1',
          rules: ['RULE_1'],
        },
        {
          sessionName: 'ANOTHER_SESSION',
          source: 'x',
          sourceUid: 'x$1',
        },
        {
          sessionName: 'ANOTHER_SESSION',
          source: 'crossref',
          sourceUid: 'crossref$10.1001/jama.2014.15912',
          rules: ['RULE_555'],
        },

      ],
      xissn: '2380-6583,2380-6591',
      xPublicationDate: '2015-01-20',
      first3AuthorNames: 'Marret Stéphane Bénichou Jacques',
      first3AuthorNamesWithInitials: 'Marret S Bénichou J',
      pageRange: '306',
      hasDoi: true,
      hasFulltext: false,
      sourceUidChain: '!hal$hal-02462375!',
    },
    technical: {
      sessionName: 'HAL_2020-12-15_2015',
      creationDate: '2020-12-15 15:56:58',
      internalId: 'udLj9TxkLzeYwHM_8ax1iq2Ix',
    },
    language: ['English'],
    classifications: { hal: { code: 'Life Sciences [q-bio]' } },
    title: {
      default: 'Antenatal Magnesium Sulfate and Outcomes for School-aged Children',
      en: 'Antenatal Magnesium Sulfate and Outcomes for School-aged Children',
    },
    authors: [
      {
        forename: 'Stéphane',
        surname: 'Marret',
        halAuthorId: '369521',
        affiliations: [
          {
            ref: 'struct-535040',
            address: 'Génomique et Médecine Personnalisée du Cancer et des Maladies Neuropsychiatriques GPMCND, Team 4 "NeoVasc" - INSERM U1245, FR',
            isni: ['0000000121083034', '0000000417859671'],
            idRef: ['198011067', '026403919', '190906332', '026388278'],
            rnsr: ['201722543J'],
          },
          {
            ref: 'struct-519394',
            address: 'Université de Rouen Normandie UNIROUEN, U1245, Institut National de la Santé et de la Recherche Médicale INSERM, Génomique et Médecine Personnalisée du Cancer et des Maladies Neuropsychiatriques GPMCND, 22, Boulevard Gambetta 76183 Rouen Cedex, FR',
            isni: ['0000000121083034', '0000000417859671'],
            idRef: ['198011067', '026403919', '190906332', '026388278'],
            rnsr: ['201722543J'],
          },
        ],
        fullname: 'Marret Stéphane',
      },
      {
        forename: 'Jacques',
        surname: 'Bénichou',
        halAuthorId: '208863',
        affiliations: [
          {
            ref: 'struct-110536',
            address: 'Université de Rouen Normandie UNIROUEN, CHU Rouen, Unité de biostatistiques [CHU Rouen], 1 rue de Germont 76031 Rouen, FR',
            isni: ['0000000121083034', '0000000417859671'],
            idRef: ['026403919', '190906332', '169975681'],
          },
        ],
        fullname: 'Bénichou Jacques',
      },
    ],
    doi: '10.1001/jama.2014.15912',
    halId: 'hal-02462375',
    originalGenre: 'ART',
    host: {
      title: 'JAMA Cardiology',
      eisbn: '978-3-319-78926-2',
      isbn: '978-3-319-78924-8',
      eissn: '2380-6591',
      issn: '2380-6583',
      electronicPublicationDate: '2017-10-23',
      publicationDate: '2015-01-20',
      issue: '3',
      language: ['English'],
      part: '10 Pt A',
      specialIssue: 'P1',
      supplement: 'Suppl 1',
      publisher: 'American Medical Association ',
      pages: { range: '306', total: 0 },
      volume: '313',
    },
    source: 'hal',
    sourceUid: 'hal$hal-02462375',
  },
  {
    business: {
      duplicates: [
        {
          // should be present coz must be found as duplicate
          sessionName: 'ANOTHER_SESSION',
          source: 'B',
          sourceUid: 'b$5',
          rules: ['RULE_66'],
        },
        {
          // should be remove coz no rules
          sessionName: 'TEST_SESSION',
          source: 'k',
          sourceUid: 'k$1',
          rules: [],
        },
        {
          // should be present coz must be found as duplicate
          sourceUid: 'hal$hal-02462375',
          internalId: 'udLj9TxkLzeYwHM_8ax1iq2Ix',
          sessionName: 'ANOTHER_SESSION',
          rules: [
            'RULE_66',
            'Article : 1ID:doi+TiC',
            'Article : 1ID:doi+TiC_ENG',
          ],
          source: 'hal',
        },
        {
          // should be present coz trans dup from b$5
          sessionName: 'ANOTHER_SESSION',
          source: 'z',
          sourceUid: 'z$2',
        },
        {
          // should be present coz trans dup from pubmed$25603006
          sessionName: 'ANOTHER_SESSION',
          source: 'x',
          sourceUid: 'x$1',
        },
        {
          // should be remove coz no rules in b$5
          sessionName: 'ANOTHER_SESSION',
          source: 'h',
          sourceUid: 'h$1',
        },
        {
          // should be remove coz no rules
          sessionName: 'TEST_SESSION',
          source: 'w',
          sourceUid: 'w$1',
        },
        {
          // should be remove coz another session
          sessionName: 'ANOTHER_SESSION',
          source: 'crossref',
          sourceUid: 'crossref$10.1001/jama.2014.10498',
          rules: ['RULE_69'],
        },
      ],
      name: 'corhal',
      duplicateGenre: 'Article',
      xissn: '0098-7484',
      xPublicationDate: '2015-01-20',
      first3AuthorNames: 'Marret Stéphane Bénichou Jacques',
      first3AuthorNamesWithInitials: 'Marret S Bénichou J',
      pageRange: '306',
      hasDoi: true,
      hasFulltext: false,
      sourceUidChain: '!b$5!crossref$10.1001/jama.2014.15912!',
    },
    technical: {
      sessionName: 'CROSSREF_2021-02-19_2015_1',
      creationDate: '2021-02-19 10:02:12',
      internalId: 'QVtJr9XOWFjIcbXWTLSXfZGN5',
    },
    language: ['English'],
    title: {
      default: 'Antenatal Magnesium Sulfate and Outcomes for School-aged Children',
      en: 'Antenatal Magnesium Sulfate and Outcomes for School-aged Children',
    },
    authors: [
      {
        forename: 'Stéphane',
        surname: 'Marret',
        affiliations: [
          {
            address: 'Department of Neonatal Medicine, Rouen University Hospital and Région-INSERM (ERI 28), Normandy University, Rouen, France',
          },
        ],
        fullname: 'Marret Stéphane',
      },
      {
        forename: 'Jacques',
        surname: 'Bénichou',
        affiliations: [
          { address: 'Department of Biostatistics and INSERM UMR 657, Normandy University, Rouen, France' },
        ],
        fullname: 'Bénichou Jacques',
      },
    ],
    doi: '10.1001/jama.2014.15912',
    originalGenre: 'Journal article',
    host: {
      title: 'JAMA',
      eisbn: '978-3-319-78926-2',
      isbn: '978-3-319-78924-8',
      eissn: '2380-6591',
      issn: '0098-7484',
      electronicPublicationDate: '2017-10-23',
      publicationDate: '2015-01-20',
      issue: '3',
      language: ['English'],
      part: '10 Pt A',
      specialIssue: 'P1',
      supplement: 'Suppl 1',
      publisher: 'American Medical Association ',
      pages: [{ range: '306-310', total: 0 }, { range: '666' }],
      volume: '313',
    },
    source: 'crossref',
    sourceUid: 'crossref$10.1001/jama.2014.15912',
  },
  {
    business: {
      duplicates: [
        {
          sessionName: 'ANOTHER_SESSION',
          source: 'crossref',
          sourceUid: 'crossref$10.1001/jama.2014.15912',
          rules: ['RULE_66'],
        },
        //{
        //  sessionName: 'TEST_SESSION',
        //  source: 'crossref',
        //  sourceUid: 'crossref$10.1001/jama.2014.66666',
        //  rules: ['RULE_66'],
        //},
        {
          sessionName: 'ANOTHER_SESSION',
          source: 'z',
          sourceUid: 'z$2',
          rules: ['RULE_69'],
        },
        {
          sessionName: 'YET_ANOTHER_SESSION',
          source: 'z',
          sourceUid: 'z$5',
          rules: ['RULE_699'],
        },
        {
          sessionName: 'ANOTHER_SESSION',
          source: 'h',
          sourceUid: 'h$1',
        },
      ],
      name: 'corhal',
      duplicateGenre: 'Article',
      xissn: '0098-7484',
      xPublicationDate: '2015-01-20',
      first3AuthorNames: 'Marret Stéphane Bénichou Jacques',
      first3AuthorNamesWithInitials: 'Marret S Bénichou J',
      pageRange: '306',
      hasDoi: true,
      hasFulltext: false,
      sourceUidChain: '!b$5!crossref$10.1001/jama.2014.15912!z$2!z$5!',
    },
    technical: {
      sessionName: 'ANOTHER_SESSION',
      creationDate: '2021-02-19 10:02:12',
      internalId: 'b$5',
    },
    language: ['English'],
    title: {
      default: 'Antenatal Magnesium Sulfate and Outcomes for School-aged Children',
      en: 'Antenatal Magnesium Sulfate and Outcomes for School-aged Children',
    },
    authors: [
      {
        forename: 'Stéphane',
        surname: 'Marret',
        affiliations: [
          {
            address: 'Department of Neonatal Medicine, Rouen University Hospital and Région-INSERM (ERI 28), Normandy University, Rouen, France',
          },
        ],
        fullname: 'Marret Stéphane',
      },
      {
        forename: 'Jacques',
        surname: 'Bénichou',
        affiliations: [
          { address: 'Department of Biostatistics and INSERM UMR 657, Normandy University, Rouen, France' },
        ],
        fullname: 'Bénichou Jacques',
      },
    ],
    doi: '10.1001/jama.2014.15912',
    originalGenre: 'Journal article',
    host: {
      title: 'JAMA',
      eisbn: '978-3-319-78926-2',
      isbn: '978-3-319-78924-8',
      eissn: '2380-6591',
      issn: '0098-7484',
      electronicPublicationDate: '2017-10-23',
      publicationDate: '2015-01-20',
      issue: '3',
      language: ['English'],
      part: '10 Pt A',
      specialIssue: 'P1',
      supplement: 'Suppl 1',
      publisher: 'American Medical Association ',
      pages: [{ range: '306-310', total: 0 }, { range: '666' }],
      volume: '313',
    },
    source: 'B',
    sourceUid: 'b$5',
  },
];
