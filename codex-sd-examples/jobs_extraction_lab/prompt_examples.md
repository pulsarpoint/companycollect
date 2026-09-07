# Job extraction examples

These examples teach field boundaries. All companies, jobs, and URLs here are
synthetic. Apply the pattern to the actual input; never return these example jobs.

A title is part of a job posting, but `title` contains only the advertised role
name. A Markdown link label may contain both the title and several other fields.
Keep seniority, specialisms, and qualifiers such as `(m/f/x)` in the title. Remove
separately displayed metadata from the title. Do not remove words just because
they can also name a department or workplace type: `Director of Engineering` and
`Remote Sensing Engineer` are complete role names.

Use the nearest applicable department heading until a new department starts.
Filter options describe available choices, not the attributes of every job.
`Remote`, `Hybrid`, and `On-site` are workplace types, not employment types.
Keep the complete location label, including geographic restrictions such as
`Remote - Canada`; do not reduce it to `Remote` or infer additional places.
Use `null` for fields that are not stated. Copy evidence as a contiguous source
quotation, not a JSON object, paraphrase, or reconstructed Markdown link.

## Example 1: A department appended to the title, with missing workplace data

The first `Engineering` belongs to `Director of Engineering`; the second is the
department. The `On-site` filter must not fill the second job's missing workplace.

source_url: `https://example.com/careers`

Input Markdown:

```markdown
# Open Positions
Filters: Location Type All Location Types Remote (1) On-site (1)
## Engineering
### [Director of Engineering Engineering • Oslo; Stockholm • Full time • Remote ](/jobs/101)
### [Interface Designer Engineering • Oslo • Full time ](/jobs/102)
```

Expected output:

```json
{"jobs":[{"title":"Director of Engineering","location":"Oslo; Stockholm","department":"Engineering","employment_type":"Full time","workplace_type":"Remote","job_url":"https://example.com/jobs/101","evidence":"Director of Engineering Engineering • Oslo; Stockholm • Full time • Remote"},{"title":"Interface Designer","location":"Oslo","department":"Engineering","employment_type":"Full time","workplace_type":null,"job_url":"https://example.com/jobs/102","evidence":"Interface Designer Engineering • Oslo • Full time"}]}
```

## Example 2: Location after the title, and two openings with the same title

The double space separates the role from the location. Different URLs identify
different openings even when their titles are identical. Neither job states an
employment type or workplace type.

source_url: `https://example.com/careers`

Input Markdown:

```markdown
## Engineering - Developer Tools
### [Quality Engineer  Dublin, Ireland  ](/jobs/201)
### [Quality Engineer  Bristol, UK  ](/jobs/202)
```

Expected output:

```json
{"jobs":[{"title":"Quality Engineer","location":"Dublin, Ireland","department":"Engineering - Developer Tools","employment_type":null,"workplace_type":null,"job_url":"https://example.com/jobs/201","evidence":"Quality Engineer  Dublin, Ireland"},{"title":"Quality Engineer","location":"Bristol, UK","department":"Engineering - Developer Tools","employment_type":null,"workplace_type":null,"job_url":"https://example.com/jobs/202","evidence":"Quality Engineer  Bristol, UK"}]}
```

## Example 3: Remote as a title word and as location metadata

`Remote Sensing` describes the engineering speciality. The trailing `Remote -
Canada` is the location label; it also explicitly states remote work. The next
department heading applies to the next opening only.

source_url: `https://example.com/careers`

Input Markdown:

```markdown
### Research
| [Remote Sensing Engineer Remote - Canada](/jobs/301) |
### Customer Success
| [Customer Success Lead Vancouver (Hybrid)](/jobs/302) |
```

Expected output:

```json
{"jobs":[{"title":"Remote Sensing Engineer","location":"Remote - Canada","department":"Research","employment_type":null,"workplace_type":"Remote","job_url":"https://example.com/jobs/301","evidence":"Remote Sensing Engineer Remote - Canada"},{"title":"Customer Success Lead","location":"Vancouver (Hybrid)","department":"Customer Success","employment_type":null,"workplace_type":"Hybrid","job_url":"https://example.com/jobs/302","evidence":"Customer Success Lead Vancouver (Hybrid)"}]}
```

## Example 4: Employment type and city joined by the Markdown conversion

In this listing, `Full TimeBerlin` contains employment type `Full Time` followed
by location `Berlin`. The gender qualifier remains part of the advertised title.

source_url: `https://example.com/careers`

Input Markdown:

```markdown
Core Functions
Finance & Legal
##### [Senior Payroll Accountant (m/f/x) Hybrid — Full TimeBerlin ](/jobs/401)
```

Expected output:

```json
{"jobs":[{"title":"Senior Payroll Accountant (m/f/x)","location":"Berlin","department":"Finance & Legal","employment_type":"Full Time","workplace_type":"Hybrid","job_url":"https://example.com/jobs/401","evidence":"Senior Payroll Accountant (m/f/x) Hybrid — Full TimeBerlin"}]}
```

## Example 5: A title within a longer description, and an incomplete continuation

The opening paragraph has no identifiable title or job link in this chunk. Do not
invent a job for it; a neighboring overlapping chunk may contain the missing
context. Extract the complete, identifiable posting below it. Its responsibilities
are description text, not part of `title`. This schema has no description field.

source_url: `https://example.com/careers`

Input Markdown:

```markdown
You will also support incident response and improve our release process.
## Design
### [Product Designer](/jobs/501)
Location: Helsinki
Employment type: Contract
You will design accessible interfaces and work with our engineering team.
```

Expected output:

```json
{"jobs":[{"title":"Product Designer","location":"Helsinki","department":"Design","employment_type":"Contract","workplace_type":null,"job_url":"https://example.com/jobs/501","evidence":"Product Designer"}]}
```

## Example 6: Filters and talent pools do not create openings

source_url: `https://example.com/careers`

Input Markdown:

```markdown
# Careers
Filter by department: Engineering, Design, Sales
Location: Remote, Oslo
No current openings.
[Join our talent community](/talent)
[Create a job alert](/alerts)
```

Expected output:

```json
{"jobs":[]}
```
