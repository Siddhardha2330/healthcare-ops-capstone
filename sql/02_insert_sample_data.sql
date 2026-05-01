INSERT INTO departments (department_name, location)
VALUES
('Cardiology', 'Block A - Floor 2'),
('Orthopedics', 'Block B - Floor 1'),
('Dermatology', 'Block C - Floor 1'),
('Pediatrics', 'Block A - Floor 3'),
('Neurology', 'Block D - Floor 2'),
('Diagnostics', 'Block E - Floor 1');

INSERT INTO doctors (doctor_name, department_id, specialization, phone, email, status)
VALUES
('Dr. Ananya Sharma', 1, 'Interventional Cardiology', '9876500001', 'ananya.sharma@hospital.com', 'Active'),
('Dr. Rohan Mehta', 1, 'General Cardiology', '9876500002', 'rohan.mehta@hospital.com', 'Active'),
('Dr. Priya Nair', 2, 'Joint Replacement', '9876500003', 'priya.nair@hospital.com', 'Active'),
('Dr. Karan Malhotra', 3, 'Clinical Dermatology', '9876500004', 'karan.malhotra@hospital.com', 'Active'),
('Dr. Neha Iyer', 4, 'Child Health', '9876500005', 'neha.iyer@hospital.com', 'Active'),
('Dr. Arjun Rao', 5, 'Neuro Medicine', '9876500006', 'arjun.rao@hospital.com', 'Active');

INSERT INTO patients (patient_name, gender, date_of_birth, phone, email, city, state)
VALUES
('Amit Kumar', 'Male', '1985-04-12', '9000000001', 'amit.kumar@email.com', 'Hyderabad', 'Telangana'),
('Sneha Reddy', 'Female', '1992-09-21', '9000000002', 'sneha.reddy@email.com', 'Hyderabad', 'Telangana'),
('Rahul Verma', 'Male', '1978-01-30', '9000000003', 'rahul.verma@email.com', 'Bengaluru', 'Karnataka'),
('Pooja Singh', 'Female', '2000-06-18', '9000000004', 'pooja.singh@email.com', 'Chennai', 'Tamil Nadu'),
('Imran Khan', 'Male', '1969-11-05', '9000000005', 'imran.khan@email.com', 'Mumbai', 'Maharashtra'),
('Kavya Menon', 'Female', '2015-03-25', '9000000006', 'kavya.menon@email.com', 'Kochi', 'Kerala'),
('Vikram Joshi', 'Male', '1988-12-10', '9000000007', 'vikram.joshi@email.com', 'Pune', 'Maharashtra'),
('Meera Das', 'Female', '1995-08-14', '9000000008', 'meera.das@email.com', 'Kolkata', 'West Bengal');

