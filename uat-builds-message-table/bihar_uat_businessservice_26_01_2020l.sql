--
-- PostgreSQL database dump
--

-- Dumped from database version 10.11
-- Dumped by pg_dump version 10.8 (Ubuntu 10.8-1.pgdg14.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

ALTER TABLE ONLY public.eg_wf_state_v2 DROP CONSTRAINT fk_eg_wf_state;
ALTER TABLE ONLY public.eg_wf_action_v2 DROP CONSTRAINT fk_eg_wf_action_v2;
DROP INDEX public.idx_pi_wf_state_v2;
DROP INDEX public.idx_pi_wf_businessservice_v2;
DROP INDEX public.idx_pi_wf_action;
ALTER TABLE ONLY public.eg_wf_state_v2 DROP CONSTRAINT uk_eg_wf_state_v2;
ALTER TABLE ONLY public.eg_wf_businessservice_v2 DROP CONSTRAINT uk_eg_wf_businessservice;
ALTER TABLE ONLY public.eg_wf_action_v2 DROP CONSTRAINT uk_eg_wf_action;
ALTER TABLE ONLY public.eg_wf_state_v2 DROP CONSTRAINT pk_eg_wf_state_v2;
ALTER TABLE ONLY public.eg_wf_businessservice_v2 DROP CONSTRAINT pk_eg_wf_businessservice;
DROP TABLE public.eg_wf_state_v2;
DROP TABLE public.eg_wf_businessservice_v2;
DROP TABLE public.eg_wf_action_v2;
SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: eg_wf_action_v2; Type: TABLE; Schema: public; Owner: biharuat
--

CREATE TABLE public.eg_wf_action_v2 (
    uuid character varying(256) NOT NULL,
    tenantid character varying(256) NOT NULL,
    currentstate character varying(256),
    action character varying(256) NOT NULL,
    nextstate character varying(256),
    roles character varying(1024) NOT NULL,
    createdby character varying(256) NOT NULL,
    createdtime bigint,
    lastmodifiedby character varying(256) NOT NULL,
    lastmodifiedtime bigint
);


ALTER TABLE public.eg_wf_action_v2 OWNER TO biharuat;

--
-- Name: eg_wf_businessservice_v2; Type: TABLE; Schema: public; Owner: biharuat
--

CREATE TABLE public.eg_wf_businessservice_v2 (
    businessservice character varying(256) NOT NULL,
    business character varying(256) NOT NULL,
    tenantid character varying(256) NOT NULL,
    uuid character varying(256) NOT NULL,
    geturi character varying(1024),
    posturi character varying(1024),
    createdby character varying(256) NOT NULL,
    createdtime bigint,
    lastmodifiedby character varying(256) NOT NULL,
    lastmodifiedtime bigint,
    businessservicesla bigint
);


ALTER TABLE public.eg_wf_businessservice_v2 OWNER TO biharuat;

--
-- Name: eg_wf_state_v2; Type: TABLE; Schema: public; Owner: biharuat
--

CREATE TABLE public.eg_wf_state_v2 (
    uuid character varying(256) NOT NULL,
    tenantid character varying(256) NOT NULL,
    businessserviceid character varying(256) NOT NULL,
    state character varying(256),
    applicationstatus character varying(256),
    sla bigint,
    docuploadrequired boolean,
    isstartstate boolean,
    isterminatestate boolean,
    createdby character varying(256) NOT NULL,
    createdtime bigint,
    lastmodifiedby character varying(256) NOT NULL,
    lastmodifiedtime bigint,
    seq integer,
    isstateupdatable boolean
);


ALTER TABLE public.eg_wf_state_v2 OWNER TO biharuat;

--
-- Data for Name: eg_wf_action_v2; Type: TABLE DATA; Schema: public; Owner: biharuat
--

COPY public.eg_wf_action_v2 (uuid, tenantid, currentstate, action, nextstate, roles, createdby, createdtime, lastmodifiedby, lastmodifiedtime) FROM stdin;
965c7cfd-3ecc-4005-a3c4-a89236a75409	bh	11444767-4060-4e43-b22b-eaca00499ade	INITIATE	9a450b16-0d80-465b-bcb2-255e6c23f96a	CITIZEN,TL_CEMP	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860
8ffb9a79-8e69-49d7-aa67-248b8866322e	bh	9a450b16-0d80-465b-bcb2-255e6c23f96a	INITIATE	9a450b16-0d80-465b-bcb2-255e6c23f96a	CITIZEN,TL_CEMP	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860
8c91033a-c04d-49df-ac8e-17e60fe191b2	bh	9a450b16-0d80-465b-bcb2-255e6c23f96a	APPLY	32770772-c9e1-42dc-957e-a4f1ca00ef3b	CITIZEN,TL_CEMP	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860
38ff1bfc-3fd8-48d6-84fa-ba396c4618d2	bh	32770772-c9e1-42dc-957e-a4f1ca00ef3b	ADHOC	32770772-c9e1-42dc-957e-a4f1ca00ef3b	TL_DOC_VERIFIER	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860
21829faa-745c-4024-8518-ffcd60cbda29	bh	32770772-c9e1-42dc-957e-a4f1ca00ef3b	FORWARD	04eca9c2-7aea-469f-8cbf-7fcc09a9f66a	TL_DOC_VERIFIER	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860
552ec6a1-6aca-4bb6-a874-3e097cd4f8e1	bh	04eca9c2-7aea-469f-8cbf-7fcc09a9f66a	APPROVE	77730ca4-8191-4799-b358-4f038de436fe	TL_APPROVER	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860
e6f5dde9-d098-4edb-af94-e865e372b634	bh	04eca9c2-7aea-469f-8cbf-7fcc09a9f66a	SENDBACK	32770772-c9e1-42dc-957e-a4f1ca00ef3b	TL_APPROVER	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860
d907be07-739e-419a-a997-013d7da2647d	bh	04eca9c2-7aea-469f-8cbf-7fcc09a9f66a	ADHOC	04eca9c2-7aea-469f-8cbf-7fcc09a9f66a	TL_APPROVER	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860
a44a7f08-29a3-409f-a965-32ae84e6d83f	bh	77730ca4-8191-4799-b358-4f038de436fe	PAY	b6d1af75-27ab-49f0-ac17-f12cb35a9c88	CITIZEN,TL_CEMP,SYSTEM_PAYMENT	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860
4856f0bb-717d-40d6-9055-73c15335d0d3	bh	04eca9c2-7aea-469f-8cbf-7fcc09a9f66a	REJECT	f50ffb6c-da78-4f52-be5b-a08e481bdd3f	TL_APPROVER	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860
54e17e44-3651-472d-a0a1-8749556d48c5	bh	32770772-c9e1-42dc-957e-a4f1ca00ef3b	REJECT	f50ffb6c-da78-4f52-be5b-a08e481bdd3f	TL_DOC_VERIFIER	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860
\.


--
-- Data for Name: eg_wf_businessservice_v2; Type: TABLE DATA; Schema: public; Owner: biharuat
--

COPY public.eg_wf_businessservice_v2 (businessservice, business, tenantid, uuid, geturi, posturi, createdby, createdtime, lastmodifiedby, lastmodifiedtime, businessservicesla) FROM stdin;
NewTL	tl-services	bh	3b3908c5-5494-44c9-9f23-66e9cfb1f9de	\N	\N	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	604800000
\.


--
-- Data for Name: eg_wf_state_v2; Type: TABLE DATA; Schema: public; Owner: biharuat
--

COPY public.eg_wf_state_v2 (uuid, tenantid, businessserviceid, state, applicationstatus, sla, docuploadrequired, isstartstate, isterminatestate, createdby, createdtime, lastmodifiedby, lastmodifiedtime, seq, isstateupdatable) FROM stdin;
11444767-4060-4e43-b22b-eaca00499ade	bh	3b3908c5-5494-44c9-9f23-66e9cfb1f9de	\N	\N	\N	f	t	f	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	8	t
9a450b16-0d80-465b-bcb2-255e6c23f96a	bh	3b3908c5-5494-44c9-9f23-66e9cfb1f9de	INITIATED	INITIATED	\N	f	t	f	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	9	t
32770772-c9e1-42dc-957e-a4f1ca00ef3b	bh	3b3908c5-5494-44c9-9f23-66e9cfb1f9de	APPLIED	APPLIED	\N	f	f	f	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	10	t
f50ffb6c-da78-4f52-be5b-a08e481bdd3f	bh	3b3908c5-5494-44c9-9f23-66e9cfb1f9de	REJECTED	REJECTED	\N	f	f	t	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	11	f
04eca9c2-7aea-469f-8cbf-7fcc09a9f66a	bh	3b3908c5-5494-44c9-9f23-66e9cfb1f9de	PENDINGAPPROVAL	PENDINGAPPROVAL	43200000	f	f	f	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	12	f
77730ca4-8191-4799-b358-4f038de436fe	bh	3b3908c5-5494-44c9-9f23-66e9cfb1f9de	PENDINGPAYMENT	PENDINGPAYMENT	43200000	f	f	f	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	13	f
b6d1af75-27ab-49f0-ac17-f12cb35a9c88	bh	3b3908c5-5494-44c9-9f23-66e9cfb1f9de	APPROVED	APPROVED	\N	f	f	t	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	aa883f8c-e094-4088-badb-ae1ae474d1ac	1578467000860	14	f
\.


--
-- Name: eg_wf_businessservice_v2 pk_eg_wf_businessservice; Type: CONSTRAINT; Schema: public; Owner: biharuat
--

ALTER TABLE ONLY public.eg_wf_businessservice_v2
    ADD CONSTRAINT pk_eg_wf_businessservice PRIMARY KEY (uuid);


--
-- Name: eg_wf_state_v2 pk_eg_wf_state_v2; Type: CONSTRAINT; Schema: public; Owner: biharuat
--

ALTER TABLE ONLY public.eg_wf_state_v2
    ADD CONSTRAINT pk_eg_wf_state_v2 PRIMARY KEY (uuid);


--
-- Name: eg_wf_action_v2 uk_eg_wf_action; Type: CONSTRAINT; Schema: public; Owner: biharuat
--

ALTER TABLE ONLY public.eg_wf_action_v2
    ADD CONSTRAINT uk_eg_wf_action PRIMARY KEY (uuid);


--
-- Name: eg_wf_businessservice_v2 uk_eg_wf_businessservice; Type: CONSTRAINT; Schema: public; Owner: biharuat
--

ALTER TABLE ONLY public.eg_wf_businessservice_v2
    ADD CONSTRAINT uk_eg_wf_businessservice UNIQUE (tenantid, businessservice);


--
-- Name: eg_wf_state_v2 uk_eg_wf_state_v2; Type: CONSTRAINT; Schema: public; Owner: biharuat
--

ALTER TABLE ONLY public.eg_wf_state_v2
    ADD CONSTRAINT uk_eg_wf_state_v2 UNIQUE (state, businessserviceid);


--
-- Name: idx_pi_wf_action; Type: INDEX; Schema: public; Owner: biharuat
--

CREATE INDEX idx_pi_wf_action ON public.eg_wf_action_v2 USING btree (action);


--
-- Name: idx_pi_wf_businessservice_v2; Type: INDEX; Schema: public; Owner: biharuat
--

CREATE INDEX idx_pi_wf_businessservice_v2 ON public.eg_wf_businessservice_v2 USING btree (businessservice);


--
-- Name: idx_pi_wf_state_v2; Type: INDEX; Schema: public; Owner: biharuat
--

CREATE INDEX idx_pi_wf_state_v2 ON public.eg_wf_state_v2 USING btree (state);


--
-- Name: eg_wf_action_v2 fk_eg_wf_action_v2; Type: FK CONSTRAINT; Schema: public; Owner: biharuat
--

ALTER TABLE ONLY public.eg_wf_action_v2
    ADD CONSTRAINT fk_eg_wf_action_v2 FOREIGN KEY (currentstate) REFERENCES public.eg_wf_state_v2(uuid);


--
-- Name: eg_wf_state_v2 fk_eg_wf_state; Type: FK CONSTRAINT; Schema: public; Owner: biharuat
--

ALTER TABLE ONLY public.eg_wf_state_v2
    ADD CONSTRAINT fk_eg_wf_state FOREIGN KEY (businessserviceid) REFERENCES public.eg_wf_businessservice_v2(uuid) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

